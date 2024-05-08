#!/usr/bin/env python3

import json
import logging
from typing import Any, Dict

import pandas as pd
from psycopg2 import Error, connect
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def read_config_file(config_file: str) -> Dict[str, Any]:
    try:
        with open(config_file, "r") as file:
            config = json.load(file)
    except FileNotFoundError:
        logging.error(f"Config file '{config_file}' not found.")
        raise
    except Exception as e:
        logging.error(f"An error occurred while reading config file: {e}")
        raise
    return config


def read_data_from_file(file_path: str) -> pd.DataFrame:
    try:
        data_df = pd.read_csv(file_path, dtype=str)
    except FileNotFoundError:
        logging.error(f"File '{file_path}' not found.")
        raise
    except Exception as e:
        logging.error(f"An error occurred while reading data from file: {e}")
        raise
    return data_df


def insert_data(conn_params: Dict[str, Any], insert_query: str, data_df: pd.DataFrame) -> None:
    try:
        conn = connect(**conn_params)
        cur = conn.cursor()

        execute_values(cur, insert_query, data_df.to_numpy())

        conn.commit()
    except Error as e:
        logging.error(f"Error inserting data: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def execute_ddl(conn_params: Dict[str, Any], ddl_statement: str) -> None:
    try:
        conn = connect(**conn_params)
        cur = conn.cursor()

        cur.execute(ddl_statement)
        conn.commit()
    except Error as e:
        logging.error(f"Error altering table: {e}")
        raise
    finally:
        cur.close()
        conn.close()


class DimDatesQueries:
    """
    Contains SQL queries related to the dim_dates table.
    """

    drop_table_query = """
    DROP TABLE IF EXISTS dim_dates;
    """

    create_table_query = """
    CREATE TABLE dim_dates (
        date_id INTEGER,
        date VARCHAR(10),
        year INTEGER,
        month INTEGER,
        month_name VARCHAR(10),
        day INTEGER,
        day_of_week VARCHAR(10)
    );
    """

    insert_query = """
    INSERT INTO dim_dates (date_id, date, year, month, month_name, day, day_of_week)
    VALUES %s;
    """

    alter_table_query = """
    ALTER TABLE dim_dates
    ADD CONSTRAINT dim_dates_pk PRIMARY KEY (date_id);
    """


class DimDistrictsQueries:
    """
    Contains SQL queries related to the dim_districts table.
    """

    drop_table_query = """
    DROP TABLE IF EXISTS dim_districts;
    """

    create_table_query = """
    CREATE TABLE dim_districts (
        district_id INTEGER,
        district_name VARCHAR(255),
        district_code VARCHAR(6),
        region_name VARCHAR(255),
        region_code VARCHAR(5),
        population FLOAT
    );
    """

    insert_query = """
    INSERT INTO dim_districts (district_id, district_name, district_code, region_name, region_code, population)
    VALUES %s;
    """

    alter_table_query = """
    ALTER TABLE dim_districts
    ADD CONSTRAINT dim_districts_pk PRIMARY KEY (district_id);
    """


class DimVaccinationStationsQueries:
    """
    Contains SQL queries related to the dim_vaccination_stations table.
    """

    drop_table_query = """
    DROP TABLE IF EXISTS dim_vaccination_stations;
    """

    create_table_query = """
    CREATE TABLE dim_vaccination_stations (
        station_id INTEGER,
        station_code VARCHAR(36),
        station_name VARCHAR(255),
        station_address VARCHAR(255),
        operational_status BOOL,
        minimal_capacity INTEGER,
        accessibility BOOL
    );
    """

    insert_query = """
    INSERT INTO dim_vaccination_stations (station_id, station_code, station_name, station_address, operational_status, minimal_capacity, accessibility)
    VALUES %s
    """

    alter_table_query = """
    ALTER TABLE dim_vaccination_stations
    ADD CONSTRAINT dim_vaccination_stations_pk PRIMARY KEY (station_id);
    """


class DimVaccinesQueries:
    """
    Contains SQL queries related to the dim_vaccines table.
    """

    drop_table_query = """
    DROP TABLE IF EXISTS dim_vaccines;
    """

    create_table_query = """
    CREATE TABLE dim_vaccines (
        vaccine_id INTEGER,
        vaccine_name VARCHAR(255),
        manufacturer VARCHAR(255),
        origin VARCHAR(255),
        technology VARCHAR(255),
        storage_temp VARCHAR(255)
    )
    """

    insert_query = """
    INSERT INTO dim_vaccines (vaccine_id, vaccine_name, manufacturer, origin, technology, storage_temp)
    VALUES %s
    """

    alter_table_query = """
    ALTER TABLE dim_vaccines
    ADD CONSTRAINT dim_vaccines_pk PRIMARY KEY (vaccine_id);
    """


class FactCovidCasesQueries:
    """
    Contains SQL queries related to the fact_covid_cases table.
    """

    drop_table_query = """
    DROP TABLE IF EXISTS fact_covid_cases;
    """

    create_table_query = """
    CREATE TABLE fact_covid_cases (
        date_ref INTEGER,
        district_ref INTEGER,
        total_cases INTEGER,
        total_cured INTEGER,
        total_deaths INTEGER,
        increase_cases INTEGER,
        percent_increase_cases FLOAT
       )
    """

    insert_query = """
    INSERT INTO fact_covid_cases (date_ref, district_ref, total_cases, total_cured, total_deaths, increase_cases, percent_increase_cases)
    VALUES %s
    """

    alter_table_query = """
    ALTER TABLE fact_covid_cases
    ADD CONSTRAINT fact_covid_cases_fk_date FOREIGN KEY (date_ref) REFERENCES dim_dates(date_id),
    ADD CONSTRAINT fact_covid_cases_fk_district FOREIGN KEY (district_ref) REFERENCES dim_districts(district_id);
    """


class FactVaccineUsageQueries:
    """
    Contains SQL queries related to the fact_vaccine_usage table.
    """

    drop_table_query = """
    DROP TABLE IF EXISTS fact_vaccine_usage;
    """

    create_table_query = """
    CREATE TABLE fact_vaccine_usage (
        date_ref INTEGER,
        station_ref INTEGER,
        district_ref INTEGER,
        vaccine_ref INTEGER,
        used_ampules INTEGER,
        spoiled_ampules INTEGER,
        administered_doses INTEGER,
        invalid_doses INTEGER
    );
    """

    insert_query = """
    INSERT INTO fact_vaccine_usage (date_ref, station_ref, district_ref, vaccine_ref, used_ampules, spoiled_ampules, administered_doses, invalid_doses)
    VALUES %s
    """

    alter_table_query = """
    ALTER TABLE fact_vaccine_usage
    ADD CONSTRAINT fact_vaccine_usage_fk_date FOREIGN KEY (date_ref) REFERENCES dim_dates(date_id),
    ADD CONSTRAINT fact_vaccine_usage_fk_station FOREIGN KEY (station_ref) REFERENCES dim_vaccination_stations(station_id),
    ADD CONSTRAINT fact_vaccine_usage_fk_district FOREIGN KEY (district_ref) REFERENCES dim_districts(district_id),
    ADD CONSTRAINT fact_vaccine_usage_fk_vaccine FOREIGN KEY (vaccine_ref) REFERENCES dim_vaccines(vaccine_id);
    """


def load_datasets(config_file: str, datasets: Dict[str, str]):
    try: 
        conn_params = read_config_file(config_file)

        # Drop tables if exist
        execute_ddl(conn_params, FactCovidCasesQueries.drop_table_query)
        execute_ddl(conn_params, FactVaccineUsageQueries.drop_table_query)
        execute_ddl(conn_params, DimDatesQueries.drop_table_query)
        execute_ddl(conn_params, DimDistrictsQueries.drop_table_query)
        execute_ddl(conn_params, DimVaccinationStationsQueries.drop_table_query)
        execute_ddl(conn_params, DimVaccinesQueries.drop_table_query)

        # (Re)create empty tables
        execute_ddl(conn_params, FactCovidCasesQueries.create_table_query)
        execute_ddl(conn_params, FactVaccineUsageQueries.create_table_query)
        execute_ddl(conn_params, DimDatesQueries.create_table_query)
        execute_ddl(conn_params, DimDistrictsQueries.create_table_query)
        execute_ddl(conn_params, DimVaccinationStationsQueries.create_table_query)
        execute_ddl(conn_params, DimVaccinesQueries.create_table_query)

        # Insert data
        data_fact_covid_cases = read_data_from_file(datasets["fact_covid_cases"])
        insert_data(conn_params, FactCovidCasesQueries.insert_query, data_fact_covid_cases)

        data_fact_vaccine_usage = read_data_from_file(datasets["fact_vaccine_usage"])
        insert_data(conn_params, FactVaccineUsageQueries.insert_query, data_fact_vaccine_usage)

        data_dim_dates = read_data_from_file(datasets["dim_dates"])
        insert_data(conn_params, DimDatesQueries.insert_query, data_dim_dates)

        data_dim_districts = read_data_from_file(datasets["dim_districts"])
        insert_data(conn_params, DimDistrictsQueries.insert_query, data_dim_districts)

        data_dim_vaccination_stations = read_data_from_file(datasets["dim_vaccination_stations"])
        insert_data(conn_params, DimVaccinationStationsQueries.insert_query, data_dim_vaccination_stations)

        data_dim_vaccines = read_data_from_file(datasets["dim_vaccines"])
        insert_data(conn_params, DimVaccinesQueries.insert_query, data_dim_vaccines)

        # Alter tables to apply integrity constraints
        execute_ddl(conn_params, DimDatesQueries.alter_table_query)
        execute_ddl(conn_params, DimDistrictsQueries.alter_table_query)
        execute_ddl(conn_params, DimVaccinationStationsQueries.alter_table_query)
        execute_ddl(conn_params, DimVaccinesQueries.alter_table_query)
        execute_ddl(conn_params, FactCovidCasesQueries.alter_table_query)
        execute_ddl(conn_params, FactVaccineUsageQueries.alter_table_query)

        logging.info("Data insertion and table alteration completed successfully.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
