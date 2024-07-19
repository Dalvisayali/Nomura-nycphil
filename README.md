# NOMURA - NY Philharmonic Performance History Analysis

The project aims to develop a personalized meal planner application that utilizes data from various sources, including nutritional databases, user input, and meal suggestion algorithms. The application will be developed using modern web and mobile technologies, leveraging frameworks such as React.js for frontend development and Node.js for backend functionality. The expected deliverables include a user-friendly interface for meal planning, calorie tracking features, personalized meal suggestions, and data visualization capabilities.

## Documentation

[![codelabs](https://img.shields.io/badge/codelabs-4285F4?style=for-the-badge&logo=codelabs&logoColor=white)](https://codelabs-preview.appspot.com/?file_id=1r6Cg_miHqOiVv43CM6GhOtq1ZWK9lf6mIlYW7VNuSVk)

## Steps to make it run on your machine

- Clone the github repository on local
- Create env file in ‘./airflow/config’; ‘./backend’; and ‘./’ directories
- Create a virtual environment using ‘python -m venv ./venv’
- Make sure you have docker daemon running on your local machine
- Navigate to the project directory and run ‘docker compose build’ to build the images
- Then execute ‘docker compose up’ to run the images as containers
- To stop the running containers press Ctrl + C / Cmd + C and then execute docker compose down to remove the containers as a cleanup step

## Empty .env structure for './airflow/config', './backend', and './'

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
```Hummel,  Johann
```

### Summary
These endpoints provide valuable insights and trends from the dataset, allowing users to query specific aspects of the data, such as the number of concerts per season, the most common conductor, the number of works per composer, soloists by instrument, and the most frequent compositions by a specific composer. Each endpoint is designed to handle exceptions gracefully and return a JSON response with the relevant data or an error message.


## Data Sources

### [raw_nyc_phil.json](https://www.kaggle.com/code/jboysen/quick-tutorial-flatten-nested-json-in-pandas/input?select=raw_nyc_phil.json)

The New York Philharmonic played its first concert on December 7, 1842. Since then, it has merged with the New York Symphony, the New/National Symphony, and had a long-running summer season at New York's Lewisohn Stadium. The Performance History database documents all known concerts of all of these organizations, amounting to more than 20,000 performances.

## Tools and Technologies:

- Backend : FastAPI
- Database : Snowflake
- Automation : Airflow
