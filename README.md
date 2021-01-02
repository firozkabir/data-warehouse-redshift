# Cloud Data Warehouse with AWS Redshift
### A project submitted as work for Udacity's Data Engineering Nano Degree

## Introduction
Sparkify is a startup providing music streaming service. Their application stores data in JSON format. 

We want to use Redshift on AWS to analyze the user data and log data to better understand the listening trends of our users. 


## Star Schema

* Staging Tables - s_songs, s_events
* Fact Table - f_songplays
    - using start_time as distkey as it is a very large dimenstion and it is likely to grow rapidly but evenly as we get more event data
    - using start_time as sortkey to make "trend" analysis faster
* Dimension Tables - d_artists, d_usrs, d_songs, d_time
    - d_users.level is a sortkey to aid with analysis of who upgrades from "Free" to "Paid"
    - d_artists.name and d_songs.title are sortkeys to aid with analysis of artist trends

![star schema](model.png)


## ETL Pipeline 

* Song dataset (json) is loaded into staging table s_songs using redshift's copy command 
* Log dataset (json) is loaded into staging table s_events using redshift's copy command 
* Data from staging tables are transformed and loaded into fact and dimension tables using sql


## How to run

### Create your own Redshift cluster on AWS
* create your own redshift cluster by going into AWS Console or using AWS CLI
* You can also use my [aws-iac-redshift]('https://github.com/firozkabir/aws-iac-redshift/blob/main/README.md') project to create a cluster using Infrastructure as Code. 



### Clone this repository
```bash
git clone git@github.com:firozkabir/data-warehouse-redshift.git
```


### Add the Redshift credentials to dwh.cfg
```
[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=

[IAM_ROLE]
ARN=''

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'
```

### Install virtual Environment
```bash
cd data-warehouse-redshift
virtualenv -p /usr/bin/python3 venv
source venv/bin/activate
pip3 install -r requirements.txt
```


### Create the database schema in Redshift
```bash
./run-dwh.sh create_schema
```


### Load data into staging and run ETL process
```bash
./run-dwh.sh do_etl
```


### Don't forget to delete the cluster when you are done.