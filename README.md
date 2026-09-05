A Python utility that connects to Cloudera Manager (CM) and emails a daily status report for BDR (Backup and Disaster Recovery) replication jobs on a CDH cluster — specifically Hive metadata replication and HDFS data replication schedules.

For each configured replication schedule, the report lists the most recent run(s) with their status (running / succeeded / failed), start and end times, and relevant details (database name for Hive jobs, file counts for HDFS jobs). The report is written to a local file and then emailed to a distribution list. It's designed to be scheduled via crontab for recurring, automated reporting.

Requirements
Python 2 (uses the legacy cm_api client library)
Network access to a Cloudera Manager instance
cm_api Python package installed
An SMTP server reachable from the host running the script
Setup

Set the Cloudera Manager credentials as environment variables before running:

bash
export CM_LOGIN=admin
export CM_PASSWORD=your_password

Update the following one-time settings directly in the script for your environment:

cm_host, cm_port — Cloudera Manager host/port
bdr_cluster_name — display name of the target cluster in CM
mail_server (in send_email()) — SMTP server hostname
from_addr, to_addr — sender and recipient email addresses
Usage
bash
python generate_bdr_report.py <limit>
<limit> — how many of the most recent job runs to include per replication schedule (e.g. 1 reports only the latest run of each job).
Notes
Despite "BDR" covering backup/DR broadly, this script only reports on Hive and HDFS replication — it does not cover HBase.
Only one cluster (matched by bdr_cluster_name) is reported on per run, even if Cloudera Manager manages multiple clusters.
