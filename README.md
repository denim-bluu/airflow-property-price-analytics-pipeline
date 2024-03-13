# Find My Home

## Project Summary

This is the full data enginerring lifecycle project. The project is to collect the data from the web (e.g. Zoopla), clean the data, and store the data in the database. This processed data will be used for the analysis and visualization. ALl these tasks will be automated using the Airflow. This is a rough sketch of the project.
![rough sketch](docs/rough_sketch.png)

## Motivation

I am planning to buy my own home very soon (fingers crossed) but I would like to know the trends of the property market based on the facts (i.e. data) not by the opinions of the real estate agents. This is a good opportunity to learn the full data engineering lifecycle and apply the skills to the real world problem. (I know this is not the perfect / over-engineered / poorly designed project but I would like to learn the full lifecycle of the data engineering.)

## Data Source

- Zoopla Website

## Data Engineering Lifecycle

1. Data Ingestion / Transformation / Load
    - Collect the data from the web using Python crawler script
    - Load initially parsed data as parquet format to the S3 bucket
    - Clean the data and store the data in the S3 bucket
    - Load the data from the S3 bucket to OLAP service (e.g. Redshift)
        ![redshift_screenshot](docs/redshift_screenshot.png)
2. Serving Data
    - Create the dashboard using the BI tool (e.g. Apache Superset)
    - Apply super simple MLs to predict the price of the property
3. Orchestration / Automate the pipeline
    - Use Airflow to automate the pipeline

## Tech Stack

### Programming Language

- Python

### Object Storage (Storing Raw / Processed Data)

    - AWS S3

### Data Warehouse (For querying the data)

    - AWS Redshift
    - Apache Druid (Attepted running on the local machine with Kubernetes hit the resource limitation. Will try to run on the AWS EKS.) 

### Data Orchestration

    - Apache Airflow

### Data Analytics

    - Apache Superset (TBD)

## How to Run

1. Run Airflow Docker Container

```bash
make airflow/up
```

2. (Deprecated) Run Apache Druid

```bash
make druid/up
```
