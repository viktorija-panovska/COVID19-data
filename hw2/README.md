# ASSIGNMENT #2: Implementing ETL in Apache Airflow


## SYSTEM REQUIREMENTS
- Python (version 3.5 or newer)
- Docker Desktop



## INSTALLATION INSTRUCTIONS

1. Make sure that Docker Desktop is running.

2. In the hw2 directory, execute the command:
```
docker build . --tag mff/airflow:latest
```

3. Create folders `dags`, `logs`, `plugins`, and `config` in the hw2 directory and move the `dag_covid.py` script into the `dags` folder.

4. In the hw2 directory, execute the commands:
```
docker compose up airflow-init
docker compose up
```

3. Go to http://127.0.0.1:8080 (username: airflow, password: airflow) to access the Airflow web server.

4. To be enable connection to PostgreSQL, create a new Connection in the Airflow web server with Connection Type "Postgres", Connection Id "postgres_webik", Host "webik.ms.mff.cuni.cz", Port "5432", and your username and password.



## SCRIPT
The script `dag_covid.py` executes an Apache Airflow workflow implementing the ETL proposed in the previous homework assignment. The DAG for the workflow is scheduled to run once a week.

The majority of the tasks in the workflow are implemented using Taskflow API, with the exception of the `PostgresBulkLoadOperator`, which is implemented as a custom operator. The workflow consists of three task groups:


- **extract**: this task group consists of tasks for fetching data from the web. Each task can only begin execution once the Sensor has determined that the web resource that the data is supposed to be fetched from is available.


- **transform**: this task group consists of tasks for transforming the raw data extracted by the tasks in the previous task group into the proper tables of the data warehouse. For the fact tables, the records from the past 14 days are selected, if such records exist. If such records do not exist in the dataset, the records from 1.1.2022-14.1.2022 are selected instead.


- **load**: this task group consists of tasks for loading the data warehouse tables into a PostgresSQL database. A custom operator is utilized for bulk loading the data, which takes a dictionary of all the datasets and bulk-loads each of them.