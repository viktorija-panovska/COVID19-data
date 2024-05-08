import logging
import os
import pandas as pd
import regex as re
import requests
from io import BytesIO
from lxml import html
from typing import Any, Dict, List
from datetime import datetime, timedelta, date

from airflow.decorators import dag, task, task_group
from airflow.sensors.base import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def fetch_content(url: str, type: str) -> BytesIO:
    try:
        response = requests.get(url)
        response.raise_for_status()     # raises an exception if unable to download file content
        if type == 'csv':
            return response.content
        elif type == 'xlsx':
            return BytesIO(response.content)
        elif type == 'html':
            html_content: bytes = response.content
            return html.fromstring(html_content)
        else:
            logging.error(f"Unknown content type: {type}")
            return None
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading file from URL: {e}")
        raise

def download_csv_dataset(url: str, output: str):
    data = fetch_content(url, 'csv')
    try:
        with open(output, "wb") as f:
            f.write(data)
    except Exception as e:
        logging.error(f"Error saving data as CSV file: {e}")
        raise
    logging.info(f"Data has been saved to the file: {output}")

def save_as_csv(data: Dict[str, List[Any]], output_file: str) -> None:
    try:
        df = pd.DataFrame(data)
        df.to_csv(output_file, index=False)
    except Exception as e:
        logging.error(f"An error occurred while saving data to CSV: {e}")
        raise


def load_csv_to_dataframe(file_path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        logging.error(f'Error loading CSV file: {e}')
        raise

def save_dataframe_to_csv(dataframe: pd.DataFrame, file_path: str):
    try:
        dataframe.to_csv(file_path, index=False)
        logging.info(f'Result has been saved to the file: {file_path}')
    except Exception as e:
        logging.error(f'Error saving DataFrame to file: {e}')
        raise

def add_surrogate_key(dataframe: pd.DataFrame, ident_name: str) -> pd.DataFrame:
    try:
        dataframe[ident_name] = range(1, len(dataframe) + 1)
        return dataframe
    except Exception as e:
        logging.error(f"Error adding surrogate key: {e}")
        raise

def select_rows(dataframe: pd.DataFrame, property_name: str, property_values: List[str]) -> pd.DataFrame:
    try:
        return dataframe[dataframe[property_name].isin(property_values)]
    except Exception as e:
        logging.error(f"Error selecting rows: {e}")
        raise

def remove_rows(dataframe: pd.DataFrame, property_name: str, property_value: List[str]) -> pd.DataFrame:
    try:
        return dataframe[dataframe[property_name] != property_value]
    except Exception as e:
        logging.error(f"Error selecting rows: {e}")
        raise

def project_columns(dataframe: pd.DataFrame, columns: list) -> pd.DataFrame:
    try:
        return dataframe[columns]
    except Exception as e:
        logging.error(f"Error projecting columns: {e}")
        raise

def rename_columns(dataframe: pd.DataFrame, column_names: dict) -> pd.DataFrame:
    try:
        return dataframe.rename(columns=column_names, copy=False)
    except Exception as e:
        logging.error(f"Error renaming columns: {e}")
        raise

def format_dates(dataframe: pd.DataFrame, column: str) -> pd.DataFrame:
    try:
        dataframe[column] = (dataframe[column].str.replace(r"(\d+)-(\d+)-(\d+)", r"\3.\2.\1", regex=True))
        return dataframe
    except Exception as e:
        logging.error(f"Error formatting numbers: {e}")
        raise

def join_dataframes(first: pd.DataFrame, second: pd.DataFrame, on: list, how: str) -> pd.DataFrame:
    try:
        return pd.merge(first, second, on=on, how=how)
    except Exception as e:
        logging.error(f"Error joining data: {e}")
        raise

def normalize(dataframe: pd.DataFrame, columns: list, ident_name: str) -> pd.DataFrame:
    try:
        categories = dataframe[columns].copy()
        categories.drop_duplicates(subset=columns, inplace=True)
        categories.sort_values(by=columns, inplace=True)
        categories[ident_name] = range(1, len(categories) + 1)

        dataframe = pd.merge(dataframe, categories, on=columns, how="left")
        dataframe.drop(columns, axis=1, inplace=True)
        return dataframe, categories
    except Exception as e:
        logging.error(f"Error disaggregating columns: {e}")
        raise

def encode_vaccine_manufacturers(dataframe: pd.DataFrame, column_name: str) -> pd.DataFrame:
    try:
        manufacturer_mapping = {
            'Pfizer': 1,
            'Moderna': 2,
            'AstraZeneca': 3,
            'Johnson & Johnson': 4,
            'Gam-COVID-Vac': 5,
            'Sinovac': 6,
            'Sinopharm': 7,
            'Covaxin': 8,
            'Convidicea': 9,
            'ЭпиВакКорона': 10,
            'Johnson & Johnson': 11,
            'CoviVac': 12,
            'RBD-Dimer': 13,
            'WIBP-Cor': 14,
            'QazCovid-in': 15
        }
        dataframe[column_name] = dataframe[column_name].map(manufacturer_mapping)
        return dataframe
    except Exception as e:
        logging.error(f'Error converting values in {column_name} column: {e}')
        raise


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


class PostgresBulkLoadOperator(BaseOperator):
    """
    Custom PostgresOperator for bulk loading data into PostgreSQL.
    """

    template_fields = ("datasets")

    @apply_defaults
    def __init__(self, *, postgres_conn_id: str, datasets: Dict[str, str], **kwargs):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.datasets = datasets

    def execute(self, context):
        for table_name, file_path in self.datasets.items():
            try:
                hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
                with open(file_path, "r") as f:
                    columns = f.readline().strip().split(",")
                    copy_sql = f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH CSV HEADER"
                    hook.copy_expert(copy_sql, f.name)
            except FileNotFoundError:
                logging.error(f"File '{file_path}' not found.")
                raise
            except Exception as ex:
                logging.error(f"An error occurred while reading data from file: {ex}")
                raise



default_args = { "owner": "panovska", "retries": 3, "retry_delay": timedelta(minutes=5) }

@dag(
    dag_id="dag_covid",
    default_args=default_args,
    start_date=datetime(2024, 3, 30),
    schedule_interval="@weekly",
    catchup=False,
)
def etl_covid():

    urls = {
        "covid_cases_url": "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/kraj-okres-nakazeni-vyleceni-umrti.csv",
        "vaccination_stations_url": "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/prehled-ockovacich-mist.csv",
        "vaccine_usage_url": "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani-spotreba.csv",
        "population_url": "https://www.czso.cz/documents/10180/165591265/13006222q414.xlsx/a5e1f2e7-7d66-4487-8c88-82766c04b185?version=1.1",
        "regions_url": "https://cs.wikipedia.org/wiki/CZ-NUTS",
        "districts_url": "https://cs.wikipedia.org/wiki/Seznam_okres%C5%AF_v_%C4%8Cesku",
        "vaccines_url": "https://cs.wikipedia.org/wiki/Vakc%C3%ADna_proti_covidu-19"
    }

    @task_group(group_id="extract")
    def extract() -> Dict[str, str]:

        @task.sensor(poke_interval=60, timeout=1800, mode="poke")
        def wait_for_resource(url: str) -> PokeReturnValue:
            return PokeReturnValue(is_done=requests.get(url).status_code == 200)


        @task
        def extract_covid_cases_dataset(url: str) -> str:
            output = 'covid_cases_dataset.csv'
            download_csv_dataset(url, output)
            return output
        

        @task
        def extract_vaccination_stations_dataset(url: str) -> str:
            output = 'vaccination_stations_dataset.csv'
            download_csv_dataset(url, output)
            return output
        

        @task
        def extract_vaccine_usage_dataset(url: str) -> str:
            output = 'vaccine_usage_dataset.csv'
            download_csv_dataset(url, output)
            return output


        @task()
        def extract_population_dataset(url: str) -> str:
            output = 'population_dataset.csv'
            content = fetch_content(url, 'xlsx')
            data = { "district": [], "population": [] }
            try:
                df = pd.read_excel(content, header=None)
                for i in range(18, 108):
                    if df.iloc[i][0][-4:] != 'kraj':
                        data["district"].append(df.iloc[i][0])
                        data["population"].append(df.iloc[i][1])
            except Exception as e:
                logging.error(f"An error occurred while appending data: {e}")
                raise

            save_as_csv(data, output)
            logging.info(f"Data has been saved to the file: {output}")
            return output


        @task
        def extract_regions_dataset(url: str) -> str:
            output = 'regions_dataset.csv'
            tree = fetch_content(url, 'html')
            data = { "code": [], "region": [] }

            table = tree.xpath("//table[@class=\"wikitable\"]/tbody")[0]
            rows = table.xpath("tr")[2:-1]

            for row in rows:
                columns = row.xpath("td")[-2:]
                data['code'].append(columns[1].text_content().strip())
                data['region'].append(columns[0].text_content().strip())

            save_as_csv(data, output)
            logging.info(f"Data has been saved to the file: {output}")
            return output


        @task
        def extract_districts_dataset(url: str) -> str:
            output = 'districts_dataset.csv'
            data = { "code": [], "district": [], "region": [] }
            tree = fetch_content(url, 'html')
            rows = tree.xpath("//table")[0].xpath("tbody/tr")

            for row in rows:
                columns = row.xpath("td")

                if len(columns) > 0:
                    data['district'].append(columns[0].text_content().split(' ', maxsplit=1)[1].strip())
                    data['region'].append(columns[6].text_content())

                    district_page_tree = fetch_content('https://cs.wikipedia.org' + columns[0].xpath('a/@href')[0], 'html')
                    district_info = district_page_tree.xpath("//table")[0].xpath(("tbody/tr"))

                    for info in district_info:
                        info_col = info.xpath("td|th")
                        if len(info_col) == 2 and info_col[0].text_content().strip() == 'LAU 1':
                            data['code'].append(info_col[1].text_content().strip())

            save_as_csv(data, output)
            logging.info(f"Data has been saved to the file: {output}")
            return output


        @task
        def extract_vaccine_dataset(url: str) -> str:
            output = 'vaccine_dataset.csv'
            data = { 'vaccine': [], 'country of origin': [], 'technology': [], 'storage temperature': [] }
            tree = fetch_content(url, 'html')

            tables = tree.xpath("//table/tbody")
            rows = tables[3].xpath("tr")

            for row in rows:
                columns = row.xpath("td")
                if len(columns) > 0:
                    name = ' '.join(text.strip() for text in columns[0].xpath(".//text()"))
                    name = re.sub(r'\[\w+\]', '', name.replace('\n', ' ').strip())
                    
                    data['vaccine'].append(name)
                    data['country of origin'].append(columns[1].text.strip())
                    data['technology'].append(re.sub(r'\[\w+\]', '', columns[2].text_content().strip()))
                    data['storage temperature'].append(columns[3].text.strip())

            save_as_csv(data, output)
            logging.info(f"Data has been saved to the file: {output}")
            return output


        covid_cases_data_file = wait_for_resource(urls['covid_cases_url']) >> extract_covid_cases_dataset(urls['covid_cases_url'])
        vaccination_stations_data_file = wait_for_resource(urls['vaccination_stations_url']) >> extract_vaccination_stations_dataset(urls['vaccination_stations_url'])
        vaccine_usage_data_file = wait_for_resource(urls['vaccine_usage_url']) >> extract_vaccine_usage_dataset(urls['vaccine_usage_url'])
        population_data_file = wait_for_resource(urls['population_url']) >> extract_population_dataset(urls['population_url'])
        regions_data_file = wait_for_resource(urls['regions_url']) >> extract_regions_dataset(urls['regions_url'])
        districts_data_file = wait_for_resource(urls['districts_url']) >> extract_districts_dataset(urls['districts_url'])
        vaccine_data_file = wait_for_resource(urls['vaccines_url']) >> extract_vaccine_dataset(urls['vaccines_url'])

        return {
            "covid_cases_data_file": covid_cases_data_file,
            "vaccination_stations_data_file": vaccination_stations_data_file,
            "vaccine_usage_data_file": vaccine_usage_data_file,
            "population_data_file": population_data_file,
            "regions_data_file": regions_data_file,
            "districts_data_file": districts_data_file,
            "vaccine_data_file": vaccine_data_file
        }


    @task_group(group_id="transform")
    def transform(datasets: Dict[str, str]):

        @task(multiple_outputs=True)
        def create_dim_districts(districts_data: str, regions_data: str, population_data: str) -> Dict[str, str]:
            output = 'dim_districts.csv'
            temp = 'temp_usage_districts.csv'

            districts_df = load_csv_to_dataframe(districts_data)
            regions_df = load_csv_to_dataframe(regions_data)
            population_df = load_csv_to_dataframe(population_data)

            districts_df = rename_columns(districts_df, { 'code': 'district_code', 'district': 'district_name', 'region': 'region_name'})
            districts_df = districts_df.sort_values(by=['district_code'])

            # join regions dataset
            regions_df = rename_columns(regions_df, { 'code': 'region_code', 'region': 'region_name' })
            districts_df = join_dataframes(districts_df, regions_df, [ 'region_name' ], 'right')
            districts_df['district_code'] = districts_df['district_code'].fillna(districts_df['region_code'] + '0')
            districts_df['district_name'] = districts_df['district_name'].fillna(districts_df['region_name'])

            # join population dataset
            population_df = rename_columns(population_df, { 'district': 'district_name' })
            districts_df = join_dataframes(districts_df, population_df, [ 'district_name' ], 'left')

            # formatting
            districts_df = add_surrogate_key(districts_df, 'district_id')
            districts_df = project_columns(districts_df, [ 'district_id', 'district_name', 'district_code', 'region_name', 'region_code', 'population' ])   #just for reordering

            save_dataframe_to_csv(districts_df, output)

            # construct temporary dataset for 'fact_vaccine_usage'
            temp_usage_df = project_columns(districts_df, [ 'district_id', 'district_code' ])
            save_dataframe_to_csv(temp_usage_df, temp)

            return {
                "temp_usage_districts": temp,
                "dim_districts": output
            }


        @task
        def create_dim_vaccines(vaccine_data: str) -> str:
            output = 'dim_vaccines.csv'

            vaccine_df = load_csv_to_dataframe(vaccine_data)

            # split vaccine column into vaccine name and manufacturer
            vaccines_extract = vaccine_df['vaccine'].str.extract(r'(\w+ \w+ )?\((.+)\)(.+)?|(.+):(.+)')
            vaccines_split = vaccine_df['vaccine'].str.split(' ', n=1, expand=True)

            vaccine_df['vaccine_name'] = vaccines_extract[1].combine_first(vaccines_extract[3]).combine_first(vaccines_split[0]).str.strip()
            vaccine_df['manufacturer'] = vaccines_extract[2].combine_first(vaccines_extract[4]).combine_first(vaccines_extract[0]).combine_first(vaccines_split[1]).str.strip()

            # formatting
            vaccine_df = add_surrogate_key(vaccine_df, 'vaccine_id')
            vaccine_df = project_columns(vaccine_df, [ 'vaccine_id', 'vaccine_name', 'manufacturer', 'country of origin', 'technology', 'storage temperature' ]) # for rearranging
            vaccine_df = rename_columns(vaccine_df, { 'country of origin': 'origin', 'storage temperature': 'storage_temp' })   # for clarity

            save_dataframe_to_csv(vaccine_df, output)
            return output


        @task(multiple_outputs=True)
        def create_dim_vaccination_stations(stations_data: str) -> Dict[str, str]:
            output = 'dim_vaccination_stations.csv'
            temp = 'temp_usage_stations.csv'

            df = load_csv_to_dataframe(stations_data)
            df = df.sort_values(by = ['ockovaci_misto_id'])
            df = add_surrogate_key(df, 'station_id')

            # construct 'dim_vaccination_stations'
            stations_df = project_columns(df, [ 'station_id', 'ockovaci_misto_id', 'ockovaci_misto_nazev', 'ockovaci_misto_adresa', 'operacni_status', 'minimalni_kapacita', 'bezbarierovy_pristup' ])    
            stations_df = rename_columns(stations_df, { 
                'ockovaci_misto_id': 'station_code',
                'ockovaci_misto_nazev': 'station_name',
                'ockovaci_misto_adresa': 'station_address',
                'operacni_status': 'operational_status',
                'minimalni_kapacita': 'minimal_capacity',
                'bezbarierovy_pristup': 'accessibility'    
            })
            stations_df['accessibility'] = stations_df['accessibility'].fillna(0)
            stations_df['accessibility'] = stations_df['accessibility'].astype(int)
            save_dataframe_to_csv(stations_df, output)

            # construct temporary for 'fact_vaccine_usage'
            temp_usage_df = project_columns(df, [ 'station_id', 'ockovaci_misto_id', 'okres_nuts_kod' ])
            temp_usage_df = rename_columns(temp_usage_df, { 'ockovaci_misto_id': 'station_code', 'okres_nuts_kod': 'district_code' })
            save_dataframe_to_csv(temp_usage_df, temp)

            return {
                "temp_usage_stations": temp,
                "dim_vaccination_stations": output
            }


        @task
        def create_dim_dates(cases_file: str, usage_file: str) -> str:
            output = 'dim_dates.csv'

            cases_df = load_csv_to_dataframe(cases_file)
            usage_df = load_csv_to_dataframe(usage_file)

            # construct dates table
            dates_df = pd.merge(cases_df[['date']], usage_df[['date']], how='outer')
            dates_df = dates_df.drop_duplicates()
            dates_df = add_surrogate_key(dates_df, 'date_id')
            dates_df = project_columns(dates_df, [ 'date_id', 'date' ])     # just reordering
            dates_df = dates_df.sort_values('date')

            # update 'fact_covid_cases' with references
            cases_df = join_dataframes(dates_df, cases_df, ['date'], 'inner')
            cases_df = cases_df.drop('date', axis=1)
            cases_df = rename_columns(cases_df, { 'date_id': 'date_ref' })

            # update 'fact_vaccine_usage' with references
            usage_df = join_dataframes(dates_df, usage_df, ['date'], 'inner')
            usage_df = usage_df.drop('date', axis=1)
            usage_df = rename_columns(usage_df, { 'date_id': 'date_ref' })

            # fill date table
            date = pd.to_datetime(dates_df['date'], dayfirst=True)
            dates_df['year'] = date.dt.year
            dates_df['month'] = date.dt.month
            dates_df['month_name'] = date.dt.month_name()
            dates_df['day'] = date.dt.day
            dates_df['day_of_week'] = date.dt.day_name()

            save_dataframe_to_csv(dates_df, output)
            save_dataframe_to_csv(cases_df, cases_file)
            save_dataframe_to_csv(usage_df, usage_file)

            return output


        @task
        def create_fact_covid_cases(cases_data: str) -> str:
            output = 'fact_covid_cases.csv'
            cases_df = load_csv_to_dataframe(cases_data)

            # selection
            today = date.today()
            extra_day = (today - timedelta(days=14)).strftime('%d.%m.%Y')
            selection = select_rows(cases_df, 'datum', [ (today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(1, 15) ])
            while len(selection) < 15:
                extra_day = '31.12.2021'
                selection = select_rows(cases_df, 'datum', [ '2021-12-31' ] + [ '2022-01-' + f'{i:02d}' for i in range(1, 14) ])

            cases_df = selection
            cases_df = cases_df.dropna(subset=['okres_lau_kod'])

            # formatting
            cases_df, districts = normalize(cases_df, ['okres_lau_kod'], 'district_ref')
            cases_df = project_columns(cases_df, [ 'datum', 'district_ref', 'kumulativni_pocet_nakazenych', 'kumulativni_pocet_vylecenych', 'kumulativni_pocet_umrti' ])
            cases_df = rename_columns(cases_df, { 
                'datum': 'date', 
                'kumulativni_pocet_nakazenych': 'total_cases', 
                'kumulativni_pocet_vylecenych': 'total_cured',
                'kumulativni_pocet_umrti': 'total_deaths'
            })
            cases_df = format_dates(cases_df, 'date')

            # construction of new columns
            cases_df['increase_cases'] = cases_df['total_cases'].diff(periods=len(districts))
            cases_df = remove_rows(cases_df, 'date', extra_day)  # these were just useful to be able to calculate the increase on 1.1.2022
            cases_df['increase_cases'] = cases_df['increase_cases'].astype(int)
            cases_df['percent_increase_cases'] = round((cases_df['increase_cases'] / cases_df['total_cases']) * 100, 4)

            save_dataframe_to_csv(cases_df, output)
            return output


        @task
        def create_fact_vaccine_usage(usage_data: str, districts_temp: str, stations_temp: str) -> str:
            output = 'fact_vaccine_usage.csv'
            usage_df = load_csv_to_dataframe(usage_data)
            districts_df = load_csv_to_dataframe(districts_temp)
            stations_df = load_csv_to_dataframe(stations_temp)

            # format usage dataset
            usage_df = project_columns(usage_df, [ 'datum', 'ockovaci_misto_kod', 'vyrobce', 'pouzite_ampulky', 'znehodnocene_ampulky', 'pouzite_davky', 'znehodnocene_davky' ])
            usage_df = rename_columns(usage_df, {
                'datum': 'date',
                'ockovaci_misto_kod': 'station_code',
                'vyrobce': 'vaccine',
                'pouzite_ampulky': 'used_ampules',
                'znehodnocene_ampulky': 'spoiled_ampules', 
                'pouzite_davky': 'administered_doses',
                'znehodnocene_davky': 'invalid_doses'
            })

            # construct vaccination dataset
            vaccination_df = join_dataframes(districts_df, stations_df, [ 'district_code' ], 'inner')
            vaccination_df = join_dataframes(vaccination_df, usage_df, [ 'station_code' ], 'inner')

            today = date.today()
            selection = select_rows(vaccination_df, 'date', [ (today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(1, 14) ])
            while len(selection) < 14:
                selection = select_rows(vaccination_df, 'date', [ '2022-01-' + f'{i:02d}' for i in range(1, 14) ])

            vaccination_df = selection
            
            vaccination_df = vaccination_df.sort_values(by = 'date')
            vaccination_df = vaccination_df.dropna(subset=['administered_doses', 'invalid_doses'])
            vaccination_df['administered_doses'] = vaccination_df['administered_doses'].astype(int)
            vaccination_df['invalid_doses'] = vaccination_df['invalid_doses'].astype(int)

            # format
            vaccination_df = encode_vaccine_manufacturers(vaccination_df, 'vaccine')
            vaccination_df = project_columns(vaccination_df, [ 'date', 'station_id', 'district_id', 'vaccine', 'used_ampules', 'spoiled_ampules', 'administered_doses', 'invalid_doses' ])
            vaccination_df = rename_columns(vaccination_df, {
                'station_id': 'station_ref',
                'district_id': 'district_ref',
                'vaccine': 'vaccine_ref'
            })
            vaccination_df = format_dates(vaccination_df, 'date')

            save_dataframe_to_csv(vaccination_df, output)
            return output     


        districts_datasets = create_dim_districts(datasets['districts_data_file'], datasets['regions_data_file'], datasets['population_data_file'])
        dim_districts = districts_datasets['dim_districts']
        temp_usage_districts = districts_datasets['temp_usage_districts']

        dim_vaccines = create_dim_vaccines(datasets['vaccine_data_file'])

        stations_datasets = create_dim_vaccination_stations(datasets['vaccination_stations_data_file'])
        dim_vaccination_stations = stations_datasets['dim_vaccination_stations']
        temp_usage_stations = stations_datasets['temp_usage_stations']

        fact_covid_cases = create_fact_covid_cases(datasets['covid_cases_data_file'])
        fact_vaccine_usage = create_fact_vaccine_usage(datasets['vaccine_usage_data_file'], temp_usage_districts, temp_usage_stations)

        dim_dates = create_dim_dates(fact_covid_cases, fact_vaccine_usage)

        return {
            "dim_districts": dim_districts,
            "dim_vaccines": dim_vaccines,
            "dim_vaccination_stations": dim_vaccination_stations,
            "dim_dates": dim_dates,
            "fact_covid_cases": fact_covid_cases,
            "fact_vaccine_usage": fact_vaccine_usage
        }


    @task_group(group_id="load")
    def load(datasets: Dict[str, str]):
        def drop_table(ddl_statement: str) -> None:
            hook = PostgresHook(postgres_conn_id="postgres_webik")
            hook.run(ddl_statement)

        def create_table(ddl_statement: str) -> None:
            hook = PostgresHook(postgres_conn_id="postgres_webik")
            hook.run(ddl_statement)

        def alter_table(ddl_statement: str) -> None:
            hook = PostgresHook(postgres_conn_id="postgres_webik")
            hook.run(ddl_statement)


        @task
        def drop_tables() -> None:
            drop_table(FactCovidCasesQueries.drop_table_query)
            drop_table(FactVaccineUsageQueries.drop_table_query)
            drop_table(DimDatesQueries.drop_table_query)
            drop_table(DimDistrictsQueries.drop_table_query)
            drop_table(DimVaccinationStationsQueries.drop_table_query)
            drop_table(DimVaccinesQueries.drop_table_query)

        @task
        def create_tables() -> None:
            create_table(FactCovidCasesQueries.create_table_query)
            create_table(FactVaccineUsageQueries.create_table_query)
            create_table(DimDatesQueries.create_table_query)
            create_table(DimDistrictsQueries.create_table_query)
            create_table(DimVaccinationStationsQueries.create_table_query)
            create_table(DimVaccinesQueries.create_table_query)

        @task
        def alter_tables() -> None:
            alter_table(DimDatesQueries.alter_table_query)
            alter_table(DimDistrictsQueries.alter_table_query)
            alter_table(DimVaccinationStationsQueries.alter_table_query)
            alter_table(DimVaccinesQueries.alter_table_query)
            alter_table(FactCovidCasesQueries.alter_table_query)
            alter_table(FactVaccineUsageQueries.alter_table_query)
        
        insert_data_task = PostgresBulkLoadOperator(
            task_id="insert_data_task",
            postgres_conn_id="postgres_webik",
            datasets=datasets
        )

        drop_tables() >> create_tables() >> insert_data_task >> alter_tables()


    initial_datasets = extract()
    clean_datasets = transform(initial_datasets)
    load(clean_datasets)

dag_etl_regions = etl_covid()
