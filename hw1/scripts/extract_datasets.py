#!/usr/bin/env python3

import logging
import os
import pandas as pd
import regex as re
import requests
from io import BytesIO
from lxml import html
from typing import Any, Dict, List

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


def save_as_csv(data: Dict[str, List[Any]], output_file: str) -> None:
    try:
        df = pd.DataFrame(data)
        df.to_csv(output_file, index=False)
    except Exception as e:
        logging.error(f"An error occurred while saving data to CSV: {e}")
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


def extract_population_dataset(url: str, output: str):
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


def extract_regions_dataset(url: str, output: str):
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


def extract_districts_dataset(url: str, output: str):
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


def extract_vaccine_dataset(url: str, output: str):
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


def extract_datasets(datasets_dir: str):
    if not os.path.exists(datasets_dir):
        os.makedirs(datasets_dir)

    download_csv_dataset(url="https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/kraj-okres-nakazeni-vyleceni-umrti.csv",
                         output = os.path.join(datasets_dir, 'covid_cases_dataset.csv'))
    
    download_csv_dataset(url="https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/prehled-ockovacich-mist.csv",
                         output = os.path.join(datasets_dir, 'vaccination_stations_dataset.csv'))
    
    download_csv_dataset(url="https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani-spotreba.csv",
                         output = os.path.join(datasets_dir, 'vaccine_usage_dataset.csv'))

    extract_population_dataset(url = "https://www.czso.cz/documents/10180/165591265/13006222q414.xlsx/a5e1f2e7-7d66-4487-8c88-82766c04b185?version=1.1",
                               output = os.path.join(datasets_dir, 'population_dataset.csv'))

    extract_regions_dataset(url = "https://cs.wikipedia.org/wiki/CZ-NUTS", 
                            output = os.path.join(datasets_dir, 'regions_dataset.csv'))

    extract_districts_dataset(url = "https://cs.wikipedia.org/wiki/Seznam_okres%C5%AF_v_%C4%8Cesku", 
                              output = os.path.join(datasets_dir, 'districts_dataset.csv'))

    extract_vaccine_dataset(url = "https://cs.wikipedia.org/wiki/Vakc%C3%ADna_proti_covidu-19", 
                             output = os.path.join(datasets_dir, 'vaccine_dataset.csv'))