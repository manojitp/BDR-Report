#!/usr/bin/python

#This program sends a BDR report to an email distribution list, you may schedule it in crontab

import sys, getopt
import pprint
from cm_api.api_client import ApiResource
import smtplib
from email.mime.text import MIMEText
import datetime
import socket
from time import gmtime, strftime

#Sends the BDR report to an email distribution list
def send_email(from_addr, to_addr, mail_subject, mail_content_file):
    #This is a one time setup
    mail_server = 'your_mail_server_hostname'

    # Create a text/plain message
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
    #choosing a date format for the report
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    current_datetime = datetime.datetime.now()
    current_date = current_datetime.date()
    str_current_datetime = str(current_datetime)
    str_current_date = str(current_date)

    ### Initialize script
    mail_content_file = "/root/scripts/mail_content_{0}".format(str_current_date)
    print mail_content_file

    ### Settings to connect to BDR cluster
    #This is a one time setup
    cm_host = "cm_host"
    cm_port = "7180"
    cm_login = "admin"
    cm_password = "pwd"
    bdr_cluster_name = "your backup cluster name" 

    #This program takes one parameter called limit, which limits the most recent N instances of a job to be reported
    #to get only the most recent run set Limit to 1
    limit = 1
    if len(argv) == 1:
        usage = 'Usage: %s <limit>' % (argv[0])
        print usage
        quit(1)
    elif len(argv) == 2:
        if argv[1].isdigit():
            limit = argv[1]
        else:
            limit = 7
    else:
        limit = 1
    print 'Limit: %s' % (str(limit))

    #These variables are used later in a loop
    bdr_cluster = None
    hdfs_service = None
    hive_service = None

    ### Connect to CM
    print "\nConnecting to Cloudera Manager at " + cm_host + ":" + cm_port
    api = ApiResource(server_host=cm_host, server_port=cm_port, username=cm_login, password=cm_password)

    ### Get BDR Cluster
    clusters = api.get_all_clusters()
    for cluster in clusters:
        if cluster.displayName == bdr_cluster_name:
            bdr_cluster = cluster
            break
        if bdr_cluster is None:
            print "Error: Cluster '" + bdr_cluster_name + "' not found"
            quit(1)

    ### Get Hive Service
    service_list = bdr_cluster.get_all_services()
    for service in service_list:
        if service.type == "HIVE":
            hive_service = service
            break
    if hive_service is None:
        print "Error: Could not locate Hive Service"
        quit(1)

    ### Get HDFS Service
    service_list = bdr_cluster.get_all_services()
    for service in service_list:
        if service.type == "HDFS":
            hdfs_service = service
            break
    if hdfs_service is None:
        print "Error: Could not locate HDFS Service"
        quit(1)

    #open the mail content file for writing
    fp = open(mail_content_file, 'w')

    ### Begin: Hive Replication
    formatted_str = "\n### Begin: Hive replications ###".format()
    print formatted_str
    fp.write(formatted_str)
    #header format for hive replication
    #Status	StartTime	EndTime	Database	Message
    formatted_str = "\nStatus\tStart\tEnd\tDB\tMessage".format()
    print formatted_str
    fp.write(formatted_str)

    schedules = hive_service.get_replication_schedules()

    ## Iterate through all replication schedules
    for schedule in schedules:
        ## Get the Hive Replication Arguments
        hive_args = schedule.hiveArguments
        replicate_data = hive_args.replicateData  

        ## Get the HDFS Replication Arguments for the Hive job
        if replicate_data:
            hdfs_args = hive_args.hdfsArguments

        ## get the replication schedule ID
        id = str(schedule.id)

        ## Get the history of commands for the scheduled Hive replication
        command_history = hive_service.get_replication_command_history(schedule_id=schedule.id, limit=limit, view='full')

        ## for each replication command for this schedule
        for command in command_history:
            if command.hiveResult is None:
                continue
            hive_result =  command.hiveResult
            if hive_result.tables is None:
                continue
            tables = hive_result.tables
            database_name = ''
            for table in tables:
                database_name = table.database
                break
            start_time = command.startTime.strftime(fmt)

            result_message = ''
            if command.resultMessage:
                result_message = command.resultMessage

            if command.active:
                formatted_str = "\nRunning\t{0}\t{1}\t\t{2}".format(start_time, database_name, result_message)
                print formatted_str
                fp.write(formatted_str)
            else:
                end_time = command.endTime.strftime(fmt)
                if not command.success:
                    formatted_str = "\n****Failed\t{0}\t{1}\t{2}\t\t{3}".format(start_time, end_time, database_name, result_message)
                    print formatted_str
                    fp.write(formatted_str)
                else:
                    formatted_str = "\nSucceeded\t{0}\t{1}\t{2}\t\t{3}".format(start_time, end_time, database_name, result_message)
                    print formatted_str
                    fp.write(formatted_str)

    ##############################
    ### End: Hive replications ###
    ##############################

    ### Begin: HDFS Replication
    formatted_str = "\n\n### Begin: HDFS replications ###".format()
    print formatted_str
    fp.write(formatted_str)
    #header format for hdfs replication
    #Status	StartTime	EndTime	HDFS_Path	Message	Files_Expected	Files_Copied	Files_Skipped	Files_Failed
    formatted_str = "\nStatus\tStart\tEnd\tPath\tMessage\tFiles Expected\tFiles Copied\tFiles Skipped\tFiles Failed".format()
    print formatted_str
    fp.write(formatted_str)

    schedules = hdfs_service.get_replication_schedules()

    ### Iterate through all replication schedules
    for schedule in schedules:
        ### Get the HDFS Arguments
        hdfs_args = schedule.hdfsArguments

        ### get the replication schedule ID
        id = str(schedule.id)

        ## Get the history of commands for the scheduled HDFS replication
        command_history = hdfs_service.get_replication_command_history(schedule_id=schedule.id, limit=limit, view='full')
        for command in command_history:
            if command.hdfsResult is None:
                continue
            hdfs_result = command.hdfsResult
            start_time = command.startTime.strftime(fmt)
            source_path = hdfs_args.sourcePath

            numFilesExpected = hdfs_result.numFilesExpected
            numFilesCopied = hdfs_result.numFilesCopied
            numFilesSkipped = hdfs_result.numFilesSkipped
            numFilesCopyFailed = hdfs_result.numFilesCopyFailed

            result_message = ''
            if command.resultMessage:
                result_message = command.resultMessage
            if command.active:
                formatted_str = "\nRunning\t{0}\t{1}\t\t{2}\t{3}\t{4}\t{5}\t{6}".format(start_time, source_path, result_message, str(numFilesExpected), str(numFilesCopied), str(numFilesSkipped), str(numFilesCopyFailed))
                print formatted_str
                fp.write(formatted_str)
            else:
                end_time = command.endTime.strftime(fmt)
                if not command.success:
                    formatted_str = "\n****Failed\t{0}\t{1}\t{2}\t\t{3}\t{4}\t{5}\t{6}\t{7}".format(start_time, end_time, source_path, result_message, str(numFilesExpected), str(numFilesCopied), str(numFilesSkipped), str(numFilesCopyFailed))
                    print formatted_str
                    fp.write(formatted_str)
                else:
                    formatted_str = "\nSucceeded\t{0}\t{1}\t{2}\t\t{3}\t{4}\t{5}\t{6}\t{7}".format(start_time, end_time, source_path, result_message, str(numFilesExpected), str(numFilesCopied), str(numFilesSkipped), str(numFilesCopyFailed))
                    print formatted_str
                    fp.write(formatted_str)

    ##############################
    ### End: HDFS replications ###
    ##############################

    #print the hostname and the current time at the end of report and close the mail content file
    hostname = socket.gethostname()
    formatted_str = "\n\nCurrent Time on {0} is {1}".format(hostname, str_current_datetime)
    print formatted_str
    fp.write(formatted_str)
    fp.close()

    #send email
    from_addr = 'from address'
    to_addr = 'to address'
    mail_subject = 'Report from %s - Daily BDR Status Report %s' % (hostname, str_current_date)
    send_email(from_addr, to_addr, mail_subject, mail_content_file)

    quit(0)

if __name__ == '__main__':
  main(sys.argv[:])
