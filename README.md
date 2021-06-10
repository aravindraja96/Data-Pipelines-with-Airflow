### Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.
They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.
The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

### Project Overview
This repo provides the ETL pipeline, to ingest sparkify's music data into an AWS Redshift Data Warehouse. The ETL pipeline will be run on an hourly basis, scheduled using Airflow.


### Dependencies
First We need to install Airflow . To do so, run pip install airflow

* Run: airflow webserver -p 8080. Refer to https://airflow.apache.org/docs/stable/start.html for more details on how to get started,

* Configure aws_credentials in Airflow using access and secret access keys. (under Airflow UI >> Admin >> Connections)

* Configure redshift connection in Airflow (under Airflow UI >> Admin >> Connections)

### Database Design and ETL Pipeline
* For the schema design, the STAR schema is used as it simplifies queries and provides fast aggregations of data.

* For the ETL pipeline, Python is used as it contains libraries such as pandas, that simplifies data manipulation. It enables reading of files from S3.

* There are 2 types of data involved, song and log data. For song data, it contains information about songs and artists, which we extract from and load into users and artists dimension tables

* Log data gives the information of each user session. From log data, we extract and load into time, users dimension tables and songplays fact table.

### Running the ETL Pipeline
---
* Turning on the sparkify_music_dwh_dag DAG in Airflow UI will automatically trigger the ETL pipelines to run.
* DAG is as such (from graph view):

![DAG](snapshots/dag.PNG)
