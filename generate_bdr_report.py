#!/usr/bin/python

# generate_bdr_report.py
#
# Purpose:
#   Connects to Cloudera Manager (CM) and generates a daily status report
#   for BDR (Backup and Disaster Recovery) replication jobs on a single,
#   named cluster - specifically:
#     - Hive metadata replication schedules
#     - HDFS data replication schedules
#   For each schedule, the report lists the most recent run(s) with their
#   status (running/succeeded/failed), timing, and relevant details (e.g.
#   database name for Hive, file counts for HDFS). The report is written
#   to a local file and then emailed to a distribution list. Intended to
#   be scheduled via crontab for daily reporting.
#
# Usage:
#   export CM_LOGIN=admin
#   export CM_PASSWORD=your_password
#   python generate_bdr_report.py <limit>
#   <limit> - how many of the most recent job run(s) to report per
#             schedule (e.g. 1 = only the latest run of each job)
#
# Required environment variables:
#   CM_LOGIN    - Cloudera Manager username
#   CM_PASSWORD - Cloudera Manager password

import sys, getopt, os
import pprint
from cm_api.api_client import ApiResource
import smtplib
from email.mime.text import MIMEText
import datetime
import socket
from time import gmtime, strftime

# ---------------------------------------------------------------------------
# send_email()
# Emails the generated BDR report (read from a file) to a distribution list.
#
# Arguments:
#   from_addr         - sender email address
#   to_addr           - recipient email address (or distribution list)
#   mail_subject      - subject line for the email
#   mail_content_file - path to the file containing the report body
# ---------------------------------------------------------------------------
def send_email(from_addr, to_addr, mail_subject, mail_content_file):
    # SMTP relay/server used to send mail - set once for your environment
    mail_server = 'your_mail_server_hostname'

    # Read the report content and build a plain-text email message
    fp = open(mail_content_file, 'rb')
    msg = MIMEText(fp.read())
    fp.close()

    msg['Subject'] = mail_subject
    msg['From'] = from_addr
    msg['To'] = to_addr

    # Send the message via SMTP server, but don't include the envelope header
    server = smtplib.SMTP(mail_server)
    server.sendmail(from_addr, to_addr, msg.as_string())
    server.quit()
    return None

def main(argv):
    # Date/time format used throughout the report (e.g. "2024-01-01 12:00:00 UTC")
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    current_datetime = datetime.datetime.now()
    current_date = current_datetime.date()
    str_current_datetime = str(current_datetime)
    str_current_date = str(current_date)

    ### Initialize script
    # Report body is staged in a local file (one per day) before emailing
    mail_content_file = "/root/scripts/mail_content_{0}".format(str_current_date)
    print mail_content_file

    ### Settings to connect to BDR cluster
    # Cloudera Manager connection details and target cluster name.
    # This is a one-time setup - update these for your environment.
    cm_host = "cm_host"
    cm_port = "7180"
    bdr_cluster_name = "your backup cluster name"

    # Read credentials from the environment rather than hardcoding them here.
    # Set these before running the script, e.g.:
    #   export CM_LOGIN=admin
    #   export CM_PASSWORD=your_password
    cm_login = os.environ.get('CM_LOGIN')
    cm_password = os.environ.get('CM_PASSWORD')
    if not cm_login or not cm_password:
        print "Error: CM_LOGIN and CM_PASSWORD environment variables must be set"
        quit(1)

    # This program takes one parameter called limit, which limits the most
    # recent N instances of a job to be reported. Set limit to 1 to report
    # only the most recent run of each replication schedule.
    limit = 1
    if len(argv) == 1:
        # No limit argument was supplied - print usage and exit
        usage = 'Usage: %s <limit>' % (argv[0])
        print usage
        quit(1)
    elif len(argv) == 2:
        # A limit argument was supplied - use it if numeric, else default to 7
        if argv[1].isdigit():
            limit = argv[1]
        else:
            limit = 7
    else:
        # Too many arguments - fall back to reporting just the latest run
        limit = 1
    print 'Limit: %s' % (str(limit))

    # These variables are populated below and used throughout the report
    bdr_cluster = None
    hdfs_service = None
    hive_service = None

    ### Connect to CM
    print "\nConnecting to Cloudera Manager at " + cm_host + ":" + cm_port
    api = ApiResource(server_host=cm_host, server_port=cm_port, username=cm_login, password=cm_password)

    ### Get BDR Cluster
    # Look up the target cluster by its display name among all clusters
    # managed by this Cloudera Manager instance.
    clusters = api.get_all_clusters()
    for cluster in clusters:
        if cluster.displayName == bdr_cluster_name:
            bdr_cluster = cluster
            break
    if bdr_cluster is None:
        print "Error: Cluster '" + bdr_cluster_name + "' not found"
        quit(1)

    ### Get Hive Service
    # Find the Hive service within the target cluster
    service_list = bdr_cluster.get_all_services()
    for service in service_list:
        if service.type == "HIVE":
            hive_service = service
            break
    if hive_service is None:
        print "Error: Could not locate Hive Service"
        quit(1)

    ### Get HDFS Service
    # Find the HDFS service within the target cluster
    service_list = bdr_cluster.get_all_services()
    for service in service_list:
        if service.type == "HDFS":
            hdfs_service = service
            break
    if hdfs_service is None:
        print "Error: Could not locate HDFS Service"
        quit(1)

    # Open the report file for writing - this file becomes the email body
    fp = open(mail_content_file, 'w')

    ### Begin: Hive Replication
    # Section header for the Hive replication portion of the report
    formatted_str = "\n### Begin: Hive replications ###".format()
    print formatted_str
    fp.write(formatted_str)
    #header format for hive replication
    #Status	StartTime	EndTime	Database	Message
    formatted_str = "\nStatus\tStart\tEnd\tDB\tMessage".format()
    print formatted_str
    fp.write(formatted_str)

    # Fetch all configured Hive replication schedules for this cluster
    schedules = hive_service.get_replication_schedules()

    ## Iterate through all replication schedules
    for schedule in schedules:
        ## Get the Hive Replication Arguments
        hive_args = schedule.hiveArguments
        replicate_data = hive_args.replicateData  

        ## Get the HDFS Replication Arguments for the Hive job
        # (only relevant when the Hive schedule also replicates underlying data)
        if replicate_data:
            hdfs_args = hive_args.hdfsArguments

        ## get the replication schedule ID
        id = str(schedule.id)

        ## Get the history of commands (runs) for the scheduled Hive
        ## replication, limited to the most recent `limit` executions
        command_history = hive_service.get_replication_command_history(schedule_id=schedule.id, limit=limit, view='full')

        ## for each replication command (run) for this schedule
        for command in command_history:
            # Skip runs with no Hive-specific result data
            if command.hiveResult is None:
                continue
            hive_result =  command.hiveResult
            if hive_result.tables is None:
                continue
            tables = hive_result.tables
            # Grab the database name from the first replicated table
            # (assumes all tables in this run belong to the same database)
            database_name = ''
            for table in tables:
                database_name = table.database
                break
            start_time = command.startTime.strftime(fmt)

            result_message = ''
            if command.resultMessage:
                result_message = command.resultMessage

            if command.active:
                # Job is still running - no end time yet
                formatted_str = "\nRunning\t{0}\t{1}\t\t{2}".format(start_time, database_name, result_message)
                print formatted_str
                fp.write(formatted_str)
            else:
                end_time = command.endTime.strftime(fmt)
                if not command.success:
                    # Job finished but failed - flagged with **** for visibility
                    formatted_str = "\n****Failed\t{0}\t{1}\t{2}\t\t{3}".format(start_time, end_time, database_name, result_message)
                    print formatted_str
                    fp.write(formatted_str)
                else:
                    # Job finished successfully
                    formatted_str = "\nSucceeded\t{0}\t{1}\t{2}\t\t{3}".format(start_time, end_time, database_name, result_message)
                    print formatted_str
                    fp.write(formatted_str)

    ##############################
    ### End: Hive replications ###
    ##############################

    ### Begin: HDFS Replication
    # Section header for the HDFS replication portion of the report
    formatted_str = "\n\n### Begin: HDFS replications ###".format()
    print formatted_str
    fp.write(formatted_str)
    #header format for hdfs replication
    #Status	StartTime	EndTime	HDFS_Path	Message	Files_Expected	Files_Copied	Files_Skipped	Files_Failed
    formatted_str = "\nStatus\tStart\tEnd\tPath\tMessage\tFiles Expected\tFiles Copied\tFiles Skipped\tFiles Failed".format()
    print formatted_str
    fp.write(formatted_str)

    # Fetch all configured HDFS replication schedules for this cluster
    schedules = hdfs_service.get_replication_schedules()

    ### Iterate through all replication schedules
    for schedule in schedules:
        ### Get the HDFS Arguments (e.g. source path being replicated)
        hdfs_args = schedule.hdfsArguments

        ### get the replication schedule ID
        id = str(schedule.id)

        ## Get the history of commands (runs) for the scheduled HDFS
        ## replication, limited to the most recent `limit` executions
        command_history = hdfs_service.get_replication_command_history(schedule_id=schedule.id, limit=limit, view='full')
        for command in command_history:
            # Skip runs with no HDFS-specific result data
            if command.hdfsResult is None:
                continue
            hdfs_result = command.hdfsResult
            start_time = command.startTime.strftime(fmt)
            source_path = hdfs_args.sourcePath

            # File-level counters for this replication run
            numFilesExpected = hdfs_result.numFilesExpected
            numFilesCopied = hdfs_result.numFilesCopied
            numFilesSkipped = hdfs_result.numFilesSkipped
            numFilesCopyFailed = hdfs_result.numFilesCopyFailed

            result_message = ''
            if command.resultMessage:
                result_message = command.resultMessage
            if command.active:
                # Job is still running - no end time yet
                formatted_str = "\nRunning\t{0}\t{1}\t\t{2}\t{3}\t{4}\t{5}\t{6}".format(start_time, source_path, result_message, str(numFilesExpected), str(numFilesCopied), str(numFilesSkipped), str(numFilesCopyFailed))
                print formatted_str
                fp.write(formatted_str)
            else:
                end_time = command.endTime.strftime(fmt)
                if not command.success:
                    # Job finished but failed - flagged with **** for visibility
                    formatted_str = "\n****Failed\t{0}\t{1}\t{2}\t\t{3}\t{4}\t{5}\t{6}\t{7}".format(start_time, end_time, source_path, result_message, str(numFilesExpected), str(numFilesCopied), str(numFilesSkipped), str(numFilesCopyFailed))
                    print formatted_str
                    fp.write(formatted_str)
                else:
                    # Job finished successfully
                    formatted_str = "\nSucceeded\t{0}\t{1}\t{2}\t\t{3}\t{4}\t{5}\t{6}\t{7}".format(start_time, end_time, source_path, result_message, str(numFilesExpected), str(numFilesCopied), str(numFilesSkipped), str(numFilesCopyFailed))
                    print formatted_str
                    fp.write(formatted_str)

    ##############################
    ### End: HDFS replications ###
    ##############################

    # Append the hostname and current time as a footer, then close the report file
    hostname = socket.gethostname()
    formatted_str = "\n\nCurrent Time on {0} is {1}".format(hostname, str_current_datetime)
    print formatted_str
    fp.write(formatted_str)
    fp.close()

    # Email the finished report to the distribution list
    from_addr = 'from address'
    to_addr = 'to address'
    mail_subject = 'Report from %s - Daily BDR Status Report %s' % (hostname, str_current_date)
    send_email(from_addr, to_addr, mail_subject, mail_content_file)

    quit(0)

if __name__ == '__main__':
  main(sys.argv[:])
