# Stock Market ETL Pipeline Python Application

***Description***

In this Data Engineering project an ETL pipeline application is created that extracts the Xetra dataset from the AWS S3 source bucket on a weekly scheduled basis, creates a report using transformations and loads the transformed data to another AWS S3 target bucket.

The production environment we are going to write the ETL pipeline for consists of a GitHub Code repository, a DockerHub Image Repository, the execution platform Kubernetes and an Orchestration tool - Apache Airflow.

***What is the Source Dataset Xetra?***

Xetra stands for Exchange Electronic Trading and it is the trading platform of the Deutsche Börse Group. This dataset is derived near-time on a minute-by-minute basis from Deutsche Börse’s trading system and saved in an AWS S3 bucket available to the public for free, further details can be found [here](https://github.com/Deutsche-Boerse/dbg-pds).

## Pipeline overview

## Goals

Here are the goals stands for Exchange Electronic Trading and it is the trading platform of the Deutsche Börse Group.

:white_check_mark: Used virtual environment

:white_check_mark: Used exeption handling

:white_check_mark: Used linting

