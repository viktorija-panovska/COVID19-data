#!/usr/bin/env python3

import logging
import os
import pandas as pd
import scripts.transformations as trans

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


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


def create_dim_districts(districts_data: str, regions_data: str, population_data: str, districts_file: str, temp_file: str):
    '''
    Constructs the dim_districts table with columns ['district_id', 'district_name', 'district_code', 
    'region_name', 'region_code', 'population'] from datasets 'districts', 'regions', and 'population',
    and a temporary dataset for 'fact_vaccine_usage' with columns ['distirict_id', 'district_code']
    '''
    districts_df = load_csv_to_dataframe(districts_data)
    regions_df = load_csv_to_dataframe(regions_data)
    population_df = load_csv_to_dataframe(population_data)

    districts_df = trans.rename_columns(districts_df, { 'code': 'district_code', 'district': 'district_name', 'region': 'region_name'})
    districts_df = districts_df.sort_values(by=['district_code'])

    # join regions dataset
    regions_df = trans.rename_columns(regions_df, { 'code': 'region_code', 'region': 'region_name' })
    districts_df = trans.join_dataframes(districts_df, regions_df, [ 'region_name' ], 'right')
    districts_df['district_code'] = districts_df['district_code'].fillna(districts_df['region_code'] + '0')
    districts_df['district_name'] = districts_df['district_name'].fillna(districts_df['region_name'])

    # join population dataset
    population_df = trans.rename_columns(population_df, { 'district': 'district_name' })
    districts_df = trans.join_dataframes(districts_df, population_df, [ 'district_name' ], 'left')

    # formatting
    districts_df = trans.add_surrogate_key(districts_df, 'district_id')
    districts_df = trans.project_columns(districts_df, [ 'district_id', 'district_name', 'district_code', 'region_name', 'region_code', 'population' ])   #just for reordering

    save_dataframe_to_csv(districts_df, districts_file)

    # construct temporary dataset for 'fact_vaccine_usage'
    temp_usage_df = trans.project_columns(districts_df, [ 'district_id', 'district_code' ])
    save_dataframe_to_csv(temp_usage_df, temp_file)


def create_dim_vaccines(vaccine_data: str, vaccines_file: str):
    '''
    Constructs the dim_vaccines table with columns ['vaccine_id', 'vaccine_name', 'manufacturer', 'origin',
    'technology', 'storage_temp'] from the dataset 'vaccines' 
    '''

    vaccine_df = load_csv_to_dataframe(vaccine_data)

    # split vaccine column into vaccine name and manufacturer
    vaccines_extract = vaccine_df['vaccine'].str.extract(r'(\w+ \w+ )?\((.+)\)(.+)?|(.+):(.+)')
    vaccines_split = vaccine_df['vaccine'].str.split(' ', n=1, expand=True)

    vaccine_df['vaccine_name'] = vaccines_extract[1].combine_first(vaccines_extract[3]).combine_first(vaccines_split[0]).str.strip()
    vaccine_df['manufacturer'] = vaccines_extract[2].combine_first(vaccines_extract[4]).combine_first(vaccines_extract[0]).combine_first(vaccines_split[1]).str.strip()

    # formatting
    vaccine_df = trans.add_surrogate_key(vaccine_df, 'vaccine_id')
    vaccine_df = trans.project_columns(vaccine_df, [ 'vaccine_id', 'vaccine_name', 'manufacturer', 'country of origin', 'technology', 'storage temperature' ]) # for rearranging
    vaccine_df = trans.rename_columns(vaccine_df, { 'country of origin': 'origin', 'storage temperature': 'storage_temp' })   # for clarity

    save_dataframe_to_csv(vaccine_df, vaccines_file)


def create_dim_vaccination_stations(stations_data: str, stations_file: str, temp_file: str):
    '''
    Construct the dim_vaccination_stations table with columns ['station_id', 'station_code', 'station_name', 'station_address',
    'operational_status', 'minimal_capacity', 'accessibility' ] and a temporary table for fact_vaccine_usage with the columns
    ['station_id', 'station_code', 'region_code' ] from the dataset 'vaccination_stations'
    '''

    df = load_csv_to_dataframe(stations_data)
    df = df.sort_values(by = ['ockovaci_misto_id'])
    df = trans.add_surrogate_key(df, 'station_id')

    # construct 'dim_vaccination_stations'
    stations_df = trans.project_columns(df, [ 'station_id', 'ockovaci_misto_id', 'ockovaci_misto_nazev', 'ockovaci_misto_adresa', 'operacni_status', 'minimalni_kapacita', 'bezbarierovy_pristup' ])    
    stations_df = trans.rename_columns(stations_df, { 
        'ockovaci_misto_id': 'station_code',
        'ockovaci_misto_nazev': 'station_name',
        'ockovaci_misto_adresa': 'station_address',
        'operacni_status': 'operational_status',
        'minimalni_kapacita': 'minimal_capacity',
        'bezbarierovy_pristup': 'accessibility'    
    })
    stations_df['accessibility'] = stations_df['accessibility'].fillna(0)
    stations_df['accessibility'] = stations_df['accessibility'].astype(int)
    save_dataframe_to_csv(stations_df, stations_file)

    # construct temporary for 'fact_vaccine_usage'
    temp_usage_df = trans.project_columns(df, [ 'station_id', 'ockovaci_misto_id', 'okres_nuts_kod' ])
    temp_usage_df = trans.rename_columns(temp_usage_df, { 'ockovaci_misto_id': 'station_code', 'okres_nuts_kod': 'district_code' })
    save_dataframe_to_csv(temp_usage_df, temp_file)


def create_dim_dates(cases_file: str, usage_file: str, dates_file: str):
    '''
    Constructs the 'dim_dates' table with columns ['date_id', 'date', 'year', 'month', 'month_name', 'day', 'day_of_week']
    from all the dates found in the tables 'fact_covid_cases' and 'fact_vaccine_usage', and updates these tables to reference
    the dates
    '''

    cases_df = load_csv_to_dataframe(cases_file)
    usage_df = load_csv_to_dataframe(usage_file)

    # construct dates table
    dates_df = pd.merge(cases_df[['date']], usage_df[['date']], how='outer')
    dates_df = dates_df.drop_duplicates()
    dates_df = trans.add_surrogate_key(dates_df, 'date_id')
    dates_df = trans.project_columns(dates_df, [ 'date_id', 'date' ])     # just reordering
    dates_df = dates_df.sort_values('date')

    # update 'fact_covid_cases' with references
    cases_df = trans.join_dataframes(dates_df, cases_df, ['date'], 'inner')
    cases_df = cases_df.drop('date', axis=1)
    cases_df = trans.rename_columns(cases_df, { 'date_id': 'date_ref' })

    # update 'fact_vaccine_usage' with references
    usage_df = trans.join_dataframes(dates_df, usage_df, ['date'], 'inner')
    usage_df = usage_df.drop('date', axis=1)
    usage_df = trans.rename_columns(usage_df, { 'date_id': 'date_ref' })

    # fill date table
    date = pd.to_datetime(dates_df['date'], dayfirst=True)
    dates_df['year'] = date.dt.year
    dates_df['month'] = date.dt.month
    dates_df['month_name'] = date.dt.month_name()
    dates_df['day'] = date.dt.day
    dates_df['day_of_week'] = date.dt.day_name()

    save_dataframe_to_csv(dates_df, dates_file)
    save_dataframe_to_csv(cases_df, cases_file)
    save_dataframe_to_csv(usage_df, usage_file)


def create_fact_covid_cases(cases_data: str, cases_file: str):
    '''
    Constructs the fact_covid_cases table with columns ['date', 'district_ref', 'total_cases', 'total_cured', 
    'total_deaths', 'increase_cases', 'percent_increase_cases'] from the dataset 'covid_cases'
    '''
    cases_df = load_csv_to_dataframe(cases_data)

    # selection
    cases_df = trans.select_rows(cases_df, 'datum', [ '2021-12-31' ] + [ '2022-01-' + f'{i:02d}' for i in range(1, 15) ])
    cases_df = cases_df.dropna(subset=['okres_lau_kod'])

    # formatting
    cases_df, districts = trans.normalize(cases_df, ['okres_lau_kod'], 'district_ref')
    cases_df = trans.project_columns(cases_df, [ 'datum', 'district_ref', 'kumulativni_pocet_nakazenych', 'kumulativni_pocet_vylecenych', 'kumulativni_pocet_umrti' ])
    cases_df = trans.rename_columns(cases_df, { 
        'datum': 'date', 
        'kumulativni_pocet_nakazenych': 'total_cases', 
        'kumulativni_pocet_vylecenych': 'total_cured',
        'kumulativni_pocet_umrti': 'total_deaths'
    })
    cases_df = trans.format_dates(cases_df, 'date')

    # construction of new columns
    cases_df['increase_cases'] = cases_df['total_cases'].diff(periods=len(districts))
    cases_df = trans.remove_rows(cases_df, 'date', '31.12.2021')  # these were just useful to be able to calculate the increase on 1.1.2022
    cases_df['increase_cases'] = cases_df['increase_cases'].astype(int)
    cases_df['percent_increase_cases'] = round((cases_df['increase_cases'] / cases_df['total_cases']) * 100, 4)

    save_dataframe_to_csv(cases_df, cases_file)


def create_fact_vaccine_usage(usage_data: str, districts_temp: str, stations_temp: str, usage_file: str):
    '''
    Constructs the 'fact_vaccine_usage' table with columns [ 'date', 'station_ref', 'district_ref', 'vaccine_ref', 'used_ampules',
    'spoiled_ampules', 'administered_doses', 'invalid_doses' ] from the 'vaccine_usage' dataset and the 'temp_usage_districts'
    and 'temp_usage_stations' temporary datasets
    '''
    usage_df = load_csv_to_dataframe(usage_data)
    districts_df = load_csv_to_dataframe(districts_temp)
    stations_df = load_csv_to_dataframe(stations_temp)

    # format usage dataset
    usage_df = trans.project_columns(usage_df, [ 'datum', 'ockovaci_misto_kod', 'vyrobce', 'pouzite_ampulky', 'znehodnocene_ampulky', 'pouzite_davky', 'znehodnocene_davky' ])
    usage_df = trans.rename_columns(usage_df, {
        'datum': 'date',
        'ockovaci_misto_kod': 'station_code',
        'vyrobce': 'vaccine',
        'pouzite_ampulky': 'used_ampules',
        'znehodnocene_ampulky': 'spoiled_ampules', 
        'pouzite_davky': 'administered_doses',
        'znehodnocene_davky': 'invalid_doses'
    })

    # construct vaccination dataset
    vaccination_df = trans.join_dataframes(districts_df, stations_df, [ 'district_code' ], 'inner')
    vaccination_df = trans.join_dataframes(vaccination_df, usage_df, [ 'station_code' ], 'inner')
    vaccination_df = trans.select_rows(vaccination_df, 'date', [ '2022-01-' + f'{i:02d}' for i in range(1, 15) ])
    vaccination_df = vaccination_df.sort_values(by = 'date')
    vaccination_df = vaccination_df.dropna(subset=['administered_doses', 'invalid_doses'])
    vaccination_df['administered_doses'] = vaccination_df['administered_doses'].astype(int)
    vaccination_df['invalid_doses'] = vaccination_df['invalid_doses'].astype(int)


    # format
    vaccination_df = encode_vaccine_manufacturers(vaccination_df, 'vaccine')
    vaccination_df = trans.project_columns(vaccination_df, [ 'date', 'station_id', 'district_id', 'vaccine', 'used_ampules', 'spoiled_ampules', 'administered_doses', 'invalid_doses' ])
    vaccination_df = trans.rename_columns(vaccination_df, {
        'station_id': 'station_ref',
        'district_id': 'district_ref',
        'vaccine': 'vaccine_ref'
    })
    vaccination_df = trans.format_dates(vaccination_df, 'date')

    save_dataframe_to_csv(vaccination_df, usage_file)



def transform_datasets(datasets_dir: str, tables_dir: str):

    if not os.path.exists(tables_dir):
        os.makedirs(tables_dir)

    temp_dir = os.path.join(tables_dir, 'temp')
    if not os.path.exists(temp_dir):
        os.mkdir(temp_dir)

    create_dim_districts(districts_data = os.path.join(datasets_dir, 'districts_dataset.csv'),
                         regions_data = os.path.join(datasets_dir, 'regions_dataset.csv'),
                         population_data = os.path.join(datasets_dir, 'population_dataset.csv'),
                         districts_file = os.path.join(tables_dir, 'dim_districts.csv'),
                         temp_file = os.path.join(temp_dir, 'temp_usage_districts.csv'))
    
    create_dim_vaccines(vaccine_data = os.path.join(datasets_dir, 'vaccine_dataset.csv'),
                        vaccines_file = os.path.join(tables_dir, 'dim_vaccines.csv'))
    
    create_dim_vaccination_stations(stations_data = os.path.join(datasets_dir, 'vaccination_stations_dataset.csv'),
                                    stations_file = os.path.join(tables_dir, 'dim_vaccination_stations.csv'),
                                    temp_file = os.path.join(temp_dir, 'temp_usage_stations.csv'))

    create_fact_covid_cases(cases_data = os.path.join(datasets_dir, 'covid_cases_dataset.csv'),
                            cases_file = os.path.join(tables_dir, 'fact_covid_cases.csv'))

    create_fact_vaccine_usage(usage_data = os.path.join(datasets_dir, 'vaccine_usage_dataset.csv'),
                              districts_temp = os.path.join(temp_dir, 'temp_usage_districts.csv'),
                              stations_temp = os.path.join(temp_dir, 'temp_usage_stations.csv'),
                              usage_file = os.path.join(tables_dir, 'fact_vaccine_usage.csv'))

    create_dim_dates(cases_file = os.path.join(tables_dir, 'fact_covid_cases.csv'), 
                     usage_file = os.path.join(tables_dir, 'fact_vaccine_usage.csv'),
                     dates_file = os.path.join(tables_dir, 'dim_dates.csv'))