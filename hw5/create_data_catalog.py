#!/usr/bin/env python3

from rdflib import Graph, BNode, Literal, Namespace, URIRef
from rdflib.namespace import RDF, XSD, DCAT, DCTERMS, FOAF
import logging

NDBI = Namespace("https://webik.ms.mff.cuni.cz/~panovsv/NDBI046/resources/")
EUROVOC = Namespace("http://eurovoc.europa.eu/")
EUA = Namespace("http://publications.europa.eu/resource/authority/")
VCARD = Namespace("https://www.w3.org/TR/vcard-rdf/")


def create_catalog_description() -> Graph:
    result = Graph()
    result.bind("ndbi", NDBI)
    result.bind("rdf", RDF)
    result.bind("xsd", XSD)
    
    create_creator(result)
    create_catalog(result)
    create_dataset(result)
    create_distributions(result)
    
    return result


def create_creator(collector: Graph) -> None:
    creator = NDBI.ViktorijaPanovska
    collector.add((creator, RDF.type, FOAF.Person))
    collector.add((creator, FOAF.givenName, Literal("Viktorija Panovska", lang="en")))
    collector.add((creator, FOAF.mbox, URIRef("mailto:viktorijapanovska137@gmail.com")))


def create_catalog(collector: Graph) -> None:
    catalog = NDBI.Catalog
    collector.add((catalog, RDF.type, DCAT.Catalog))
    collector.add((catalog, DCAT.dataset, NDBI.VaccineUsageDataset))

    # Provenance metadata
    collector.add((catalog, DCTERMS.publisher, NDBI.ViktorijaPanovska))

    # Domain metadata
    collector.add((catalog, DCTERMS.title, Literal("NDBI046 Data Catalog Summer 2024", lang="en")))


def create_dataset(collector: Graph) -> None:
    vaccine_usage_dataset = NDBI.VaccineUsageDataset
    collector.add((vaccine_usage_dataset, RDF.type, DCAT.Dataset))
    collector.add((vaccine_usage_dataset, DCAT.distribution, NDBI.VaccineUsageDatasetCSV))
    collector.add((vaccine_usage_dataset, DCAT.distribution, NDBI.VaccineUsageDatasetTTL))

    # Provenance metadata
    collector.add((vaccine_usage_dataset, DCTERMS.publisher, NDBI.ViktorijaPanovska))
    collector.add((vaccine_usage_dataset, DCTERMS.creator, NDBI.ViktorijaPanovska))
    collector.add((vaccine_usage_dataset, DCTERMS.issued, Literal("2024-04-01T16:00:00", datatype=XSD.dateTime)))
    collector.add((vaccine_usage_dataset, DCTERMS.modified, Literal("2024-04-22T16:40:00", datatype=XSD.dateTime)))
    
    contact_point = BNode()
    collector.add((vaccine_usage_dataset, DCAT.contactPoint, contact_point))
    collector.add((contact_point, RDF.type, VCARD.Individual))
    collector.add((contact_point, VCARD.fn, Literal("Viktorija Panovska", lang="en")))
    collector.add((contact_point, VCARD.hasEmail, URIRef("mailto:viktorijapanovska137@gmail.com")))

    # Domain metadata
    collector.add((vaccine_usage_dataset, DCTERMS.title, Literal("Usage of different types of COVID vaccines by vaccination stations in each district of the Czech Republic in the period between 1.1.2022 and 14.1.2022", lang="en")))
    collector.add((vaccine_usage_dataset, DCTERMS.description, Literal("This dataset consists of data on the number of used and spoiled ampules and the number of administered and invalid doses of different types of COVID vaccines by each vaccination station (which there is data for) in each district of the Czech Republic for every day between 1.1.2022 and 14.1.2022.", lang="en")))
    collector.add((vaccine_usage_dataset, DCAT.keyword, Literal("COVID-19", lang="en")))
    collector.add((vaccine_usage_dataset, DCAT.keyword, Literal("coronavirus", lang="en")))
    collector.add((vaccine_usage_dataset, DCAT.keyword, Literal("vaccination", lang="en")))
    collector.add((vaccine_usage_dataset, DCAT.keyword, Literal("vaccine", lang="en")))
    collector.add((vaccine_usage_dataset, DCAT.theme, EUROVOC["1854"]))           # disease prevention
    collector.add((vaccine_usage_dataset, DCAT.theme, EUROVOC["4635"]))           # vaccine
    collector.add((vaccine_usage_dataset, DCAT.theme, EUROVOC["4636"]))           # vaccination
    collector.add((vaccine_usage_dataset, DCAT.theme, EUROVOC["c_814bb9e4"]))     # coronavirus disease
    collector.add((vaccine_usage_dataset, DCTERMS.spatial, EUA["country/CZE"]))

    periodOfTime = BNode()
    collector.add((vaccine_usage_dataset, DCTERMS.temporal, periodOfTime))
    collector.add((periodOfTime, RDF.type, DCTERMS.PeriodOfTime))
    collector.add((periodOfTime, DCAT.startDate, Literal("2022-01-01", datatype=XSD.date)))
    collector.add((periodOfTime, DCAT.endDate, Literal("2022-01-14", datatype=XSD.date)))

    # Business metadata
    collector.add((vaccine_usage_dataset, DCTERMS.license, Literal('https://gitlab.mff.cuni.cz/teaching/ndbi046/2023-24/viktorija-panovska/-/blob/main/hw5/License.txt?ref_type=heads', datatype=XSD.anyURI)))
    collector.add((vaccine_usage_dataset, DCTERMS.accessRights, EUA["access-right/PUBLIC"]))


def create_distributions(collector: Graph) -> None:
    csv_distribution = NDBI.VaccineUsageDatasetCSV
    collector.add((csv_distribution, RDF.type, DCAT.Distribution))

    # # Technical metadata
    collector.add((csv_distribution, DCAT.downloadURL, URIRef("https://webik.ms.mff.cuni.cz/~62533848/fact_vaccine_usage.csv")))
    collector.add((csv_distribution, DCTERMS.format, EUA["file-type/CSV"]))
    collector.add((csv_distribution, DCAT.mediaType, URIRef("https://www.iana.org/assignments/media-types/text/csv")))
    collector.add((csv_distribution, DCAT.byteSize, Literal("29547", datatype=XSD.nonNegativeInteger)))

    csv_access_service = BNode()
    collector.add((csv_distribution, DCAT.accessService, csv_access_service))
    collector.add((csv_access_service, RDF.type, DCAT.DataService))
    collector.add((csv_access_service, DCTERMS.type, URIRef("http://purl.org/dc/dcmitype/Dataset")))
    collector.add((csv_access_service, DCAT.endpointURL, URIRef("https://webik.ms.mff.cuni.cz/~62533848/fact_vaccine_usage.csv")))
    collector.add((csv_access_service, DCAT.servesDataset, NDBI.VaccineUsageDataset))

    # Domain metadata
    collector.add((csv_distribution, DCTERMS.title, Literal("CSV Distribution of 'Usage of different types of COVID vaccines by vaccination stations in each district of the Czech Republic in the period between 1.1.2022 and 14.1.2022' Dataset", lang="en")))

    # Business metadata
    collector.add((csv_distribution, DCTERMS.accrualPeriodicity, EUA["frequency/WEEKLY"]))
    collector.add((csv_distribution, DCTERMS.license, URIRef('https://gitlab.mff.cuni.cz/teaching/ndbi046/2023-24/viktorija-panovska/-/blob/main/hw2/License.txt?ref_type=heads')))
    collector.add((csv_distribution, DCTERMS.accessRights, EUA["access-right/PUBLIC"]))

    ttl_distribution = NDBI.VaccineUsageDatasetTTL
    collector.add((ttl_distribution, RDF.type, DCAT.Distribution))

    # # Technical metadata
    collector.add((ttl_distribution, DCAT.downloadURL, URIRef("https://webik.ms.mff.cuni.cz/~62533848/data_cube.ttl")))
    collector.add((ttl_distribution, DCTERMS.format, EUA["file-type/RDF_TURTLE"]))
    collector.add((ttl_distribution, DCAT.mediaType, URIRef("https://www.iana.org/assignments/media-types/text/turtle")))
    collector.add((ttl_distribution, DCAT.byteSize, Literal("309614", datatype=XSD.nonNegativeInteger)))

    ttl_access_service = BNode()
    collector.add((ttl_distribution, DCAT.accessService, ttl_access_service))
    collector.add((ttl_access_service, RDF.type, DCAT.DataService))
    collector.add((ttl_access_service, DCTERMS.type, URIRef("http://purl.org/dc/dcmitype/Dataset")))
    collector.add((ttl_access_service, DCAT.endpointURL, URIRef("https://webik.ms.mff.cuni.cz/~62533848/data_cube.ttl")))
    collector.add((ttl_access_service, DCAT.servesDataset, NDBI.VaccineUsageDataset))

    # Domain metadata
    collector.add((ttl_distribution, DCTERMS.title, Literal("RDF Turtle Distribution of 'Usage of different types of COVID vaccines by vaccination stations in each district of the Czech Republic in the period between 1.1.2022 and 14.1.2022' Dataset", lang="en")))
    
    # Business metadata
    collector.add((ttl_distribution, DCTERMS.license, Literal('https://gitlab.mff.cuni.cz/teaching/ndbi046/2023-24/viktorija-panovska/-/blob/main/hw3/License.txt?ref_type=heads', datatype=XSD.anyURI)))
    collector.add((ttl_distribution, DCTERMS.accessRights, EUA["access-right/PUBLIC"]))


if __name__ == "__main__":
    try:
        data_catalog = create_catalog_description()
        if data_catalog is not None:
            data_catalog.serialize(format='turtle', destination='data_catalog.ttl')
            logging.info('Data catalog serialization successful.')
    except Exception as e:
        logging.error(f'An error occurred: {e}')