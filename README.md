# NOMURA - NY Philharmonic Performance History Analysis

This project involves converting raw concert data into a structured, normalized relational database, loading it into a chosen database system, and creating APIs to publish various aggregates and analyses of the dataset. The goal is to transform the data into a format that supports efficient querying and analysis, enabling insightful reports and data-driven decision-making.

#### Objectives
1. **Data Conversion and Normalization**:
    - Convert the raw concert data into a set of relational tables.
    - Normalize the data to eliminate redundancy and ensure data integrity.
    - Define appropriate relationships between tables using foreign keys.

2. **Database Loading**:
    - Load the normalized tables into a database system of your choice (e.g. Snowflake).
    - Ensure data consistency and integrity during the loading process.

3. **API Development**:
    - Create APIs to publish various aggregates and analyses of the dataset.
    - Provide endpoints that allow users to input parameters and get meaningful insights.
    - Handle errors gracefully and return informative error messages.


## Steps to make it run on your machine

- Clone the github repository on local
- Create env file in the Project file

```
# Used in DAG
SNOWFLAKE_USER = ''
SNOWFLAKE_PASSWORD = ''
SNOWFLAKE_ACCOUNT = ''
SNOWFLAKE_WAREHOUSE = ''
SNOWFLAKE_DATABASE = ''
SNOWFLAKE_SCHEMA = ''

# Used in docker-compose.yaml
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=./airflow
```

- Make sure you have docker daemon running on your local machine
- Run the make command to build and deploy
```
make build-up
```
- Login to airflow dashboard at `http://0.0.0.0:8080/home`
```
Username: airflow
Password: airflow
```

- Trigger the `sandbox` pipeline
- View the graph od `sandbox` pipeline and click on "Logs" tab to check the successful execution
- Navigate to `http://0.0.0.0:8095/docs#` to access the FastAPI Swagger!

## Airflow pipeline

### Pipeline Overview

1. **Load Data**: Loaded JSON data and normalized it into Pandas DataFrames.
2. **Clean Data**: Cleaned the data and handled null values. Converted the columns to appropriate datatypes making it ready to store.
3. **Export to CSV**: Temporarily exported the cleaned dataframes to CSVs, preparing for bulk update
3. **Snowflake Operations**: Created Snowflake tables and insert cleaned data csv by first staging it and then copying into tables.

### Run

- Navigate to `http://0.0.0.0:8080/home`
- Username:  `airflow`
- Password: `airflow`

## FastAPI

This webserver provided analysis/trends by connecting to the Snowflake DB.
Here is a description of each API endpoint:

### API Endpoints Description

#### 1. **Concerts Per Season**
**Endpoint**: `/api/v1/concerts_per_season/`

**Description**: This endpoint retrieves the number of concerts performed in each season. It provides an overview of how many concerts were held across different seasons.


#### 2. **Most Common Conductor**
**Endpoint**: `/api/v1/most_common_conductor/`

**Description**: This endpoint identifies the most common conductor, i.e., the conductor who has conducted the most performances. It provides insight into which conductor has been most frequently associated with the performances.


#### 3. **Works Per Composer**
**Endpoint**: `/api/v1/works_per_composer/`

**Description**: This endpoint retrieves the number of works performed for each composer. It provides an analysis of the distribution of performances across different composers.


#### 4. **Soloists By Instrument**
**Endpoint**: `/api/v1/soloists_by_instrument/`

**Description**: This endpoint retrieves soloists who played a specific instrument. It allows users to see which soloists have performed with a given instrument.


#### 5. **Most Frequent Compositions By Composer**
**Endpoint**: `/api/v1/most_frequent_compositions_by_composer/`

**Description**: This endpoint retrieves the most frequently performed compositions by a specific composer. It provides insight into which compositions by a particular composer have been performed the most.

**Sample Input**:
```
Hummel,  Johann
```

### Summary
These endpoints provide valuable insights and trends from the dataset, allowing users to query specific aspects of the data, such as the number of concerts per season, the most common conductor, the number of works per composer, soloists by instrument, and the most frequent compositions by a specific composer. Each endpoint is designed to handle exceptions gracefully and return a JSON response with the relevant data or an error message.


## Data Sources

### [raw_nyc_phil.json](https://www.kaggle.com/code/jboysen/quick-tutorial-flatten-nested-json-in-pandas/input?select=raw_nyc_phil.json)

The New York Philharmonic played its first concert on December 7, 1842. Since then, it has merged with the New York Symphony, the New/National Symphony, and had a long-running summer season at New York's Lewisohn Stadium. The Performance History database documents all known concerts of all of these organizations, amounting to more than 20,000 performances.

## Tools and Technologies:

This project implements an ETL pipeline using Apache Airflow for data cleaning, Snowflake for data storage, and FastAPI to expose the backend. Each component has been carefully chosen to leverage its strengths in handling specific aspects of the pipeline.

#### Component Responsibilities

1. **Apache Airflow**
   - **Data Loading**: Reads raw JSON data and normalizes it into structured Pandas DataFrames.
   - **Data Cleaning**: Processes the data to handle null values, convert data types, and ensure data integrity.
   - **Workflow Management**: Manages the sequence and dependencies of tasks, ensuring that each step of the ETL process is executed in the correct order.

2. **Snowflake**
   - **Data Storage**: Stores the cleaned and structured data, making it available for querying and analysis.
   - **Data Warehousing**: Provides a scalable and efficient platform for storing large volumes of data, with support for complex queries and integration with various data tools.

3. **FastAPI**
   - **Backend API**: Exposes the processed data through a RESTful API, allowing external systems and users to access and interact with the data.
   - **Interactive Documentation**: Provides automatically generated and interactive API documentation, making it easy for developers to understand and use the API endpoints.


