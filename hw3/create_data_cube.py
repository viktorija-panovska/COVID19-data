#!/usr/bin/env python3

import logging
import pandas as pd
import sys

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from rdflib import Graph, BNode, Literal, Namespace
from rdflib.namespace import RDF, QB, XSD, SKOS, DCTERMS, OWL

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s') 


NS = Namespace('https://webik.ms.mff.cuni.cz/~panovsv/NDBI046/ontology#')
NSR = Namespace('https://webik.ms.mff.cuni.cz/~panovsv/NDBI046/resources/')
RDFS = Namespace('http://www.w3.org/2000/01/rdf-schema#')
SDMX_DIMENSION = Namespace('http://purl.org/linked-data/sdmx/2009/dimension#')
SDMX_CONCEPT = Namespace('http://purl.org/linked-data/sdmx/2009/concept#')
SDMX_MEASURE = Namespace('http://purl.org/linked-data/sdmx/2009/measure#')
SDMX_CODE = Namespace('http://purl.org/linked-data/sdmx/2009/code#')


def get_dataframe(username: str, password: str) -> pd.DataFrame:
    try:
        engine = create_engine(URL.create(
            drivername='postgresql',
            username=username,
            password=password,
            host='webik.ms.mff.cuni.cz',
            port='5432',
            database='ndbi046'
        ))

        sql_query = '''
            SELECT * 
            FROM fact_vaccine_usage
            JOIN dim_dates ON fact_vaccine_usage.date_ref = dim_dates.date_id
            JOIN dim_vaccination_stations ON fact_vaccine_usage.station_ref = dim_vaccination_stations.station_id
            JOIN dim_districts ON fact_vaccine_usage.district_ref = dim_districts.district_id
            JOIN dim_vaccines ON fact_vaccine_usage.vaccine_ref = dim_vaccines.vaccine_id
        '''

        df = pd.read_sql_query(sql_query, engine)
        engine.dispose()
        return df
    except Exception as e:
        logging.error(f'Error fetching data to CSV: {e}')


def as_data_cube(data: pd.DataFrame) -> Graph:
    try:
        result = Graph()

        # Bind namespaces to prefixes
        result.bind('ndbi', NS)
        result.bind('ndbi-r', NSR)
        result.bind('rdfs', RDFS)
        result.bind('rdf', RDF)
        result.bind('xsd', XSD)
        result.bind('owl', OWL)
        result.bind('sdmx-code', SDMX_CODE)
        result.bind('sdmx-measure', SDMX_MEASURE)
        result.bind('sdmx-concept', SDMX_CONCEPT)
        result.bind('sdmx-dimension', SDMX_DIMENSION)

        create_concept_schemes(result)
        create_concept_classes(result)
        create_concepts(result, data)

        dimensions = create_dimensions(result)
        measures = create_measures(result)
        structure = create_structure(result, dimensions, measures)

        dataset = create_dataset(result, structure)
        create_observations(result, dataset, data)

        return result
    except Exception as e:
        logging.error(f'Error creating Data Cube: {e}')
        return Graph()


def create_concept_schemes(collector: Graph) -> None:
    try:
        date = SDMX_CODE.date
        collector.add((date, RDF.type, SKOS.ConceptScheme))
        collector.add((date, SKOS.prefLabel, Literal('Date', lang='en')))
        collector.add((date, RDFS.label, Literal('Date', lang='en')))
        collector.add((date, SKOS.note, Literal('This code list provides a list of dates that records are available for.', lang='en')))
        collector.add((date, RDFS.seeAlso, NSR.Date))

        district = SDMX_CODE.district
        collector.add((district, RDF.type, SKOS.ConceptScheme))
        collector.add((district, SKOS.prefLabel, Literal('District', lang='en')))
        collector.add((district, RDFS.label, Literal('District', lang='en')))
        collector.add((district, SKOS.note, Literal('This code list provides a list of districts in the Czech Republic.', lang='en')))
        collector.add((district, RDFS.seeAlso, NSR.District))

        station = SDMX_CODE.station
        collector.add((station, RDF.type, SKOS.ConceptScheme))        
        collector.add((station, SKOS.prefLabel, Literal('Station', lang='en')))
        collector.add((station, RDFS.label, Literal('Station', lang='en')))
        collector.add((station, SKOS.note, Literal('This code list provides a list of vaccination stations in the Czech Republic for which there is data.', lang='en')))
        collector.add((station, RDFS.seeAlso, NSR.Station))

        vaccine = SDMX_CODE.vaccine
        collector.add((vaccine, RDF.type, SKOS.ConceptScheme))
        collector.add((vaccine, SKOS.prefLabel, Literal('Vaccine', lang='en')))
        collector.add((vaccine, RDFS.label, Literal('Vaccine', lang='en')))
        collector.add((vaccine, SKOS.note, Literal('This code list provides a list of COVID vaccines.', lang='en')))
        collector.add((vaccine, RDFS.seeAlso, NSR.Vaccine))

    except Exception as e:
        logging.error(f'Error creating concept schemes: {e}')

def create_concept_classes(collector: Graph) -> None:
    try:
        date = SDMX_CODE.Date
        collector.add((date, RDF.type, RDFS.Class))
        collector.add((date, RDF.type, OWL.Class))
        collector.add((date, RDFS.subClassOf, SKOS.Concept))
        collector.add((date, RDFS.label, Literal('Date', lang='en')))
        collector.add((date, SKOS.prefLabel, Literal('Date', lang='en')))
        collector.add((date, RDFS.seeAlso, NSR.date))

        district = SDMX_CODE.District
        collector.add((district, RDF.type, RDFS.Class))
        collector.add((district, RDF.type, OWL.Class))
        collector.add((district, RDFS.subClassOf, SKOS.Concept))
        collector.add((district, RDFS.label, Literal('District', lang='en')))
        collector.add((district, SKOS.prefLabel, Literal('District', lang='en')))
        collector.add((district, RDFS.seeAlso, NSR.district))

        station = SDMX_CODE.Station
        collector.add((station, RDF.type, RDFS.Class))
        collector.add((station, RDF.type, OWL.Class))
        collector.add((station, RDFS.subClassOf, SKOS.Concept))
        collector.add((station, RDFS.label, Literal('Station', lang='en')))
        collector.add((station, SKOS.prefLabel, Literal('Station', lang='en')))
        collector.add((station, RDFS.seeAlso, NSR.station))

        vaccine = SDMX_CODE.Vaccine
        collector.add((vaccine, RDF.type, RDFS.Class))
        collector.add((vaccine, RDF.type, OWL.Class))
        collector.add((vaccine, RDFS.subClassOf, SKOS.Concept))
        collector.add((vaccine, RDFS.label, Literal('Vaccine', lang='en')))
        collector.add((vaccine, SKOS.prefLabel, Literal('Vaccine', lang='en')))
        collector.add((vaccine, RDFS.seeAlso, NSR.vaccine))

    except Exception as e:
        logging.error(f'Error creating resource classes: {e}')

def create_concepts(collector: Graph, data: pd.DataFrame) -> None:
    try:
        # Create resources for date
        date_categories = data.drop_duplicates(subset=['date_id'])[['year', 'month', 'day', 'date_id']]
        for _, date_row in date_categories.iterrows():
            date_resource = NSR[f'date/{date_row['date_id']}']
            collector.add((date_resource, RDF.type, SKOS.Concept))
            collector.add((date_resource, RDF.type, SDMX_CODE.Date))
            collector.add((date_resource, SKOS.topConceptOf, SDMX_CODE.date))
            collector.add((date_resource, SKOS.prefLabel, Literal(f'{date_row['year']}-{date_row['month']:02d}-{date_row['day']:02d}', datatype=XSD.date)))
            collector.add((date_resource, SKOS.inScheme, SDMX_CODE.date))

        # Create resources for districts
        district_categories = data.drop_duplicates(subset=['district_id'])[['district_name', 'district_id']]
        for _, district_row in district_categories.iterrows():
            district_resource = NSR[f'district/{district_row['district_id']}']
            collector.add((district_resource, RDF.type, SKOS.Concept))
            collector.add((district_resource, RDF.type, SDMX_CODE.District))
            collector.add((district_resource, SKOS.topConceptOf, SDMX_CODE.district))
            collector.add((district_resource, SKOS.prefLabel, Literal(district_row['district_name'], lang='cs')))
            collector.add((district_resource, SKOS.inScheme, SDMX_CODE.district))

        # Create resources for vaccination stations
        station_categories = data.drop_duplicates(subset=['station_id'])[['station_name', 'station_id']]
        for _, station_row in station_categories.iterrows():
            station_resource = NSR[f'station/{station_row['station_id']}']
            collector.add((station_resource, RDF.type, SKOS.Concept))
            collector.add((station_resource, RDF.type, SDMX_CODE.Station))
            collector.add((station_resource, SKOS.topConceptOf, SDMX_CODE.station))
            collector.add((station_resource, SKOS.prefLabel, Literal(station_row['station_name'], lang='cs')))
            collector.add((station_resource, SKOS.inScheme, SDMX_CODE.station))

        # Create resources for vaccines
        vaccine_categories = data.drop_duplicates(subset=['vaccine_id'])[['vaccine_name', 'vaccine_id']]
        for _, vaccine_row in vaccine_categories.iterrows():
            vaccine_resource = NSR[f'vaccine/{vaccine_row['vaccine_id']}']
            collector.add((vaccine_resource, RDF.type, SKOS.Concept))
            collector.add((vaccine_resource, RDF.type, SDMX_CODE.Vaccine))
            collector.add((vaccine_resource, SKOS.topConceptOf, SDMX_CODE.vaccine))
            collector.add((vaccine_resource, SKOS.prefLabel, Literal(vaccine_row['vaccine_name'], lang='en')))
            collector.add((vaccine_resource, SKOS.inScheme, SDMX_CODE.vaccine))

    except Exception as e:
        logging.error(f'Error creating resources: {e}')
        return {}


def create_dimensions(collector: Graph) -> list:
    try:
        date = NS.date
        collector.add((date, RDF.type, RDFS.Property))
        collector.add((date, RDF.type, QB.DimensionProperty))
        collector.add((date, RDFS.label, Literal('Date', lang='en')))
        collector.add((date, RDFS.subPropertyOf, SDMX_CODE.Date))
        collector.add((date, RDFS.range, SDMX_CODE.Date))
        collector.add((date, QB.codeList, SDMX_CODE.date))

        district = NS.district
        collector.add((district, RDF.type, RDFS.Property))
        collector.add((district, RDF.type, QB.DimensionProperty))
        collector.add((district, RDFS.label, Literal('District', lang='en')))
        collector.add((district, RDFS.subPropertyOf, SDMX_CODE.District))
        collector.add((district, RDFS.range, SDMX_CODE.District))
        collector.add((district, QB.codeList, SDMX_CODE.district))

        station = NS.station
        collector.add((station, RDF.type, RDFS.Property))
        collector.add((station, RDF.type, QB.DimensionProperty))
        collector.add((station, RDFS.label, Literal('Station', lang='en')))
        collector.add((station, RDFS.subPropertyOf, SDMX_CONCEPT.Station))
        collector.add((station, RDFS.range, SDMX_CODE.Station))
        collector.add((station, QB.codeList, SDMX_CODE.station))

        vaccine = NS.vaccine
        collector.add((vaccine, RDF.type, RDFS.Property))
        collector.add((vaccine, RDF.type, QB.DimensionProperty))
        collector.add((vaccine, RDFS.label, Literal('Vaccine', lang='en')))
        collector.add((vaccine, RDFS.subPropertyOf, SDMX_CODE.Vaccine))
        collector.add((vaccine, RDFS.range, SDMX_CODE.Vaccine))
        collector.add((vaccine, QB.codeList, SDMX_CODE.vaccine))

        return [date, district, station, vaccine]
    except Exception as e:
        logging.error(f'Error creating dimensions: {e}')
        return []

def create_measures(collector: Graph) -> list:
    try:
        used_ampules = NS.used_ampules
        collector.add((used_ampules, RDF.type, RDFS.Property))
        collector.add((used_ampules, RDF.type, QB.MeasureProperty))
        collector.add((used_ampules, RDFS.label, Literal('Total vaccine ampules used', lang='en')))
        collector.add((used_ampules, RDFS.range, XSD.int))
        collector.add((used_ampules, RDFS.subPropertyOf, SDMX_MEASURE.obsValue))

        spoiled_ampules = NS.spoiled_ampules
        collector.add((spoiled_ampules, RDF.type, RDFS.Property))
        collector.add((spoiled_ampules, RDF.type, QB.MeasureProperty))
        collector.add((spoiled_ampules, RDFS.label, Literal('Total vaccine ampules spoiled', lang='en')))
        collector.add((spoiled_ampules, RDFS.range, XSD.int))
        collector.add((spoiled_ampules, RDFS.subPropertyOf, SDMX_MEASURE.obsValue))

        administered_doses = NS.administered_doses
        collector.add((administered_doses, RDF.type, RDFS.Property))
        collector.add((administered_doses, RDF.type, QB.MeasureProperty))
        collector.add((administered_doses, RDFS.label, Literal('Total administered doses', lang='en')))
        collector.add((administered_doses, RDFS.range, XSD.int))
        collector.add((administered_doses, RDFS.subPropertyOf, SDMX_MEASURE.obsValue))

        invalid_doses = NS.invalid_doses
        collector.add((invalid_doses, RDF.type, RDFS.Property))
        collector.add((invalid_doses, RDF.type, QB.MeasureProperty))
        collector.add((invalid_doses, RDFS.label, Literal('Total invalid doses', lang='en')))
        collector.add((invalid_doses, RDFS.range, XSD.int))
        collector.add((invalid_doses, RDFS.subPropertyOf, SDMX_MEASURE.obsValue))

        return [used_ampules, spoiled_ampules, administered_doses, invalid_doses]
    except Exception as e:
        logging.error(f'Error creating measures: {e}')
        return []

def create_structure(collector: Graph, dimensions: list, measures: list) -> BNode:
    try:
        # define slice
        sliceByStation = NS.slice_station
        collector.add((sliceByStation, RDF.type, QB.SliceKey))
        collector.add((sliceByStation, RDFS.label, Literal('Slice by station', lang='en')))
        collector.add((sliceByStation, RDFS.comment, Literal('This slice groups stations together, fixing date, district, and vaccine values', lang='en')))
        collector.add((sliceByStation, QB.componentProperty, NS.date))
        collector.add((sliceByStation, QB.componentProperty, NS.district))
        collector.add((sliceByStation, QB.componentProperty, NS.vaccine))

        # define structure
        structure = NS.structure
        collector.add((structure, RDF.type, QB.DataStructureDefinition))

        for index, dimension in enumerate(dimensions):
            component = BNode()
            collector.add((structure, QB.component, component))
            collector.add((component, QB.dimension, dimension))
            collector.add((component, QB.order, Literal(index + 1)))

            if index != 2:
                collector.add((component, QB.componentAttachment, QB.Slice))

        for measure in measures:
            component = BNode()
            collector.add((structure, QB.component, component))
            collector.add((component, QB.measure, measure))

        collector.add((structure, QB.sliceKey, sliceByStation))
        return structure
    except Exception as e:
        logging.error(f'Error creating structure: {e}')
        return BNode()
    
def create_dataset(collector: Graph, structure: BNode) -> BNode:
    try:
        # take the stations that are in Prague and on 01.01.2022 have vaccinated with the Pfizer vaccine
        slicePrague = NS.slice_prague
        collector.add((slicePrague, RDF.type, QB.Slice))
        collector.add((slicePrague, QB.sliceStructure, NS.slice_station))
        collector.add((slicePrague, NS.date, NSR[f'date/15']))
        collector.add((slicePrague, NS.district, NSR[f'date/1']))
        collector.add((slicePrague, NS.vaccine, NSR[f'date/1']))

        dataset = NSR.dataCubeInstance
        collector.add((dataset, RDF.type, QB.DataSet))
        collector.add((dataset, QB.structure, structure))
        collector.add((dataset, QB.slice, slicePrague))

        # Metadata
        collector.add((dataset, DCTERMS.title, Literal('COVID vaccine usage in the Czech Republic', lang='en')))
        collector.add((dataset, RDFS.label, Literal('COVID vaccine usage in the Czech Republic', lang='en')))
        collector.add((dataset, DCTERMS.description, Literal('This data cube consists of data regarding the usage of COVID vaccines by vaccination stations in different districts in the Czech Republic in the last 14 days, or if no such data is available, in the time period between 1.1.2022 and 14.1.2022', lang='en')))
        collector.add((dataset, RDFS.comment, Literal('This data cube consists of data regarding the usage of COVID vaccines by vaccination stations in different districts in the Czech Republic in the last 14 days, or if no such data is available, in the time period between 1.1.2022 and 14.1.2022', lang='en')))
        collector.add((dataset, DCTERMS.issued, Literal('2024-04-22', datatype=XSD.date)))
        collector.add((dataset, DCTERMS.modified, Literal('2024-04-22', datatype=XSD.date)))
        collector.add((dataset, DCTERMS.publisher, Literal('Viktorija Panovska', lang='en')))
        collector.add((dataset, DCTERMS.license, Literal('https://gitlab.mff.cuni.cz/teaching/ndbi046/2023-24/viktorija-panovska/-/blob/main/hw3/License.txt?ref_type=heads', datatype=XSD.anyURI)))

        return dataset

    except Exception as e:
        logging.error(f'Error creating dataset: {e}')

def create_observations(collector: Graph, dataset: BNode, data: pd.DataFrame) -> None:
    try:
        for index, row in data.iterrows():
            resource = NSR['observation-' + str(index).zfill(3)]

            collector.add((resource, RDF.type, QB.Observation))
            collector.add((resource, QB.dataSet, dataset))
            
            # Dimensions
            collector.add((resource, NS.station, NSR[f'station/{row['station_id']}']))

            if (row['date_id'] == 15 and row['district_id'] == 1 and row['vaccine_id'] == 1):
                collector.add((NS.slice_prague, QB.observation, resource))
            else:
                collector.add((resource, NS.date, NSR[f'date/{row['date_id']}']))
                collector.add((resource, NS.district, NSR[f'district/{row['district_id']}']))
                collector.add((resource, NS.vaccine, NSR[f'vaccine/{row['vaccine_id']}']))

            # Measures
            collector.add((resource, NS.used_ampules, Literal(row['used_ampules'], datatype=XSD.int)))
            collector.add((resource, NS.spoiled_ampules, Literal(row['spoiled_ampules'], datatype=XSD.int)))
            collector.add((resource, NS.administered_doses, Literal(row['administered_doses'], datatype=XSD.int)))
            collector.add((resource, NS.invalid_doses, Literal(row['invalid_doses'], datatype=XSD.int)))

        logging.info('Observations created successfully.')
    except Exception as e:
        logging.error(f'Error creating observations: {e}')


if __name__ == '__main__':

    if len(sys.argv) != 3:
        logging.error('Usage: python data_cube.py <username> <password>')
        sys.exit(1)

    try:
        data = get_dataframe(sys.argv[1], sys.argv[2])
        if data is not None:
            data_cube = as_data_cube(data)
            if data_cube is not None:
                data_cube.serialize(format='turtle', destination='data_cube.ttl')
                logging.info('Data Cube serialization successful.')
    except Exception as e:
        logging.error(f'An error occurred: {e}')