#!/usr/bin/env python3

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, FOAF, XSD, PROV
import logging

NS = Namespace("https://webik.ms.mff.cuni.cz/~panovsv/NDBI046#")


def create_prov_data() -> Graph:
    result = Graph()

    result.bind("", NS)
    result.bind("rdf", RDF)
    result.bind("xsd", XSD)
    
    create_entities(result)
    create_agents(result)
    create_activities(result)
    
    return result


def create_entities(collector: Graph) -> None:
    covid_cases_dataset = NS.CovidCasesDataset
    collector.add((covid_cases_dataset, RDF.type, PROV.Entity))
    collector.add((covid_cases_dataset, PROV.wasAttributedTo, NS.CzechMinistryOfHealth))
    collector.add((covid_cases_dataset, PROV.atLocation, Literal("https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/kraj-okres-nakazeni-vyleceni-umrti.csv", datatype=XSD.anyURI)))

    vaccination_stations_dataset = NS.VaccinationStationsDataset
    collector.add((vaccination_stations_dataset, RDF.type, PROV.Entity))
    collector.add((vaccination_stations_dataset, PROV.wasAttributedTo, NS.CzechMinistryOfHealth))
    collector.add((vaccination_stations_dataset, PROV.atLocation, Literal("https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/prehled-ockovacich-mist.csv", datatype=XSD.anyURI)))

    vaccine_usage_dataset = NS.VaccineUsageDataset
    collector.add((vaccine_usage_dataset, RDF.type, PROV.Entity))
    collector.add((vaccine_usage_dataset, PROV.wasAttributedTo, NS.CzechMinistryOfHealth))
    collector.add((vaccine_usage_dataset, PROV.atLocation, Literal("https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani-spotreba.csv", datatype=XSD.anyURI)))

    population_dataset = NS.PopulationDataset
    collector.add((population_dataset, RDF.type, PROV.Entity))
    collector.add((population_dataset, PROV.wasAttributedTo, NS.CzechStatisticalOffice))
    collector.add((population_dataset, PROV.atLocation, Literal("https://www.czso.cz/documents/10180/165591265/13006222q414.xlsx/a5e1f2e7-7d66-4487-8c88-82766c04b185?version=1.1", datatype=XSD.anyURI)))

    regions_webpage = NS.RegionsWebpage
    collector.add((regions_webpage, RDF.type, PROV.Entity))
    collector.add((regions_webpage, PROV.wasAttributedTo, NS.Wikipedia))
    collector.add((regions_webpage, PROV.atLocation, Literal("https://cs.wikipedia.org/wiki/CZ-NUTS", datatype=XSD.anyURI)))

    districts_webpage = NS.DistrictsWebpage
    collector.add((districts_webpage, RDF.type, PROV.Entity))
    collector.add((districts_webpage, PROV.wasAttributedTo, NS.Wikipedia))
    collector.add((districts_webpage, PROV.atLocation, Literal("https://cs.wikipedia.org/wiki/Seznam_okres%C5%AF_v_%C4%8Cesku", datatype=XSD.anyURI)))

    vaccines_webpage = NS.VaccinesWebpage
    collector.add((vaccines_webpage, RDF.type, PROV.Entity))
    collector.add((vaccines_webpage, PROV.wasAttributedTo, NS.Wikipedia))
    collector.add((vaccines_webpage, PROV.atLocation, Literal("https://cs.wikipedia.org/wiki/Vakc%C3%ADna_proti_covidu-19", datatype=XSD.anyURI)))


    dim_districts = NS.DimDistricts
    collector.add((dim_districts, RDF.type, PROV.Entity))
    collector.add((dim_districts, PROV.wasGeneratedBy, NS.ApacheAirflowActivity))
    collector.add((dim_districts, PROV.wasAttributedTo, NS.ViktorijaPanovska))
    collector.add((dim_districts, PROV.wasDerivedFrom, NS.DistrictsWebpage))
    collector.add((dim_districts, PROV.wasDerivedFrom, NS.RegionsWebpage))
    collector.add((dim_districts, PROV.hadPrimarySource, NS.PopulationDataset))

    dim_vaccines = NS.DimVaccines
    collector.add((dim_vaccines, RDF.type, PROV.Entity))
    collector.add((dim_vaccines, PROV.wasGeneratedBy, NS.ApacheAirflowActivity))
    collector.add((dim_vaccines, PROV.wasAttributedTo, NS.ViktorijaPanovska))
    collector.add((dim_vaccines, PROV.wasDerivedFrom, NS.VaccinesWebpage))

    dim_vaccination_stations = NS.DimVaccinationStations
    collector.add((dim_vaccination_stations, RDF.type, PROV.Entity))
    collector.add((dim_vaccination_stations, PROV.wasGeneratedBy, NS.ApacheAirflowActivity))
    collector.add((dim_vaccination_stations, PROV.wasAttributedTo, NS.ViktorijaPanovska))
    collector.add((dim_vaccination_stations, PROV.hadPrimarySource, NS.VaccinationStationsDataset))

    dim_dates = NS.DimDates
    collector.add((dim_dates, RDF.type, PROV.Entity))
    collector.add((dim_dates, PROV.wasGeneratedBy, NS.ApacheAirflowActivity))
    collector.add((dim_dates, PROV.wasAttributedTo, NS.ViktorijaPanovska))
    collector.add((dim_dates, PROV.wasDerivedFrom, NS.FactCovidCases))
    collector.add((dim_dates, PROV.wasDerivedFrom, NS.FactVaccineUsage))
    
    temp_usage_districts = NS.TempUsageDistricts
    collector.add((temp_usage_districts, RDF.type, PROV.Entity))
    collector.add((temp_usage_districts, PROV.wasGeneratedBy, NS.ApacheAirflowActivity))
    collector.add((temp_usage_districts, PROV.wasAttributedTo, NS.ViktorijaPanovska))
    collector.add((temp_usage_districts, PROV.wasDerivedFrom, NS.DimDistricts))

    temp_usage_stations = NS.TempUsageStations
    collector.add((temp_usage_stations, RDF.type, PROV.Entity))
    collector.add((temp_usage_stations, PROV.wasGeneratedBy, NS.ApacheAirflowActivity))
    collector.add((temp_usage_stations, PROV.wasAttributedTo, NS.ViktorijaPanovska))
    collector.add((temp_usage_stations, PROV.wasDerivedFrom, NS.DimVaccinationStations))

    fact_covid_cases = NS.FactCovidCases
    collector.add((fact_covid_cases, RDF.type, PROV.Entity))
    collector.add((fact_covid_cases, PROV.wasGeneratedBy, NS.ApacheAirflowActivity))
    collector.add((fact_covid_cases, PROV.wasAttributedTo, NS.ViktorijaPanovska))
    collector.add((fact_covid_cases, PROV.hadPrimarySource, NS.CovidCasesDataset))

    fact_vaccine_usage = NS.FactVaccineUsage
    collector.add((fact_vaccine_usage, RDF.type, PROV.Entity))
    collector.add((fact_vaccine_usage, PROV.wasGeneratedBy, NS.ApacheAirflowActivity))
    collector.add((fact_vaccine_usage, PROV.wasAttributedTo, NS.ViktorijaPanovska))
    collector.add((fact_vaccine_usage, PROV.hadPrimarySource, NS.VaccineUsageDataset))
    collector.add((fact_vaccine_usage, PROV.wasDerivedFrom, NS.TempUsageDistricts))
    collector.add((fact_vaccine_usage, PROV.wasDerivedFrom, NS.TempUsageStations))


    data_cube = NS.DataCube
    collector.add((data_cube, RDF.type, PROV.Entity))
    collector.add((data_cube, PROV.wasGeneratedBy, NS.DataCubeActivity))
    collector.add((data_cube, PROV.wasAttributedTo, NS.ViktorijaPanovska))
    collector.add((data_cube, PROV.wasDerivedFrom, NS.FactVaccineUsage))
    collector.add((data_cube, PROV.wasDerivedFrom, NS.DimDates))
    collector.add((data_cube, PROV.wasDerivedFrom, NS.DimDistricts))
    collector.add((data_cube, PROV.wasDerivedFrom, NS.DimVaccinationStations))
    collector.add((data_cube, PROV.wasDerivedFrom, NS.DimVaccines))


    administered_invalid_doses_visualization = NS.AdministeredInvalidDosesVisualizationActivity
    collector.add((administered_invalid_doses_visualization, RDF.type, PROV.Entity))
    collector.add((administered_invalid_doses_visualization, PROV.wasGeneratedBy, NS.BarGraphVisualizationActivity))
    collector.add((administered_invalid_doses_visualization, PROV.wasAttributedTo, NS.ViktorijaPanovska))
    collector.add((administered_invalid_doses_visualization, PROV.wasDerivedFrom, NS.FactVaccineUsage))
    collector.add((administered_invalid_doses_visualization, PROV.wasDerivedFrom, NS.DimDates))
    collector.add((administered_invalid_doses_visualization, PROV.wasDerivedFrom, NS.DimDistricts))
    collector.add((administered_invalid_doses_visualization, PROV.wasDerivedFrom, NS.DimVaccinationStations))

    cases_deaths_visualization = NS.CasesDeathsVisualization
    collector.add((cases_deaths_visualization, RDF.type, PROV.Entity))
    collector.add((cases_deaths_visualization, PROV.wasGeneratedBy, NS.ScatterplotVisualizationActivity))
    collector.add((cases_deaths_visualization, PROV.wasAttributedTo, NS.ViktorijaPanovska))
    collector.add((cases_deaths_visualization, PROV.wasDerivedFrom, NS.FactCovidCases))
    collector.add((cases_deaths_visualization, PROV.wasDerivedFrom, NS.DimDistricts))


def create_agents(collector: Graph) -> None:
    apache_airflow = NS.ApacheAirflow
    collector.add((apache_airflow, RDF.type, PROV.SoftwareAgent))
    collector.add((apache_airflow, RDF.type, PROV.Agent))
    collector.add((apache_airflow, PROV.actedOnBehalfOf, NS.ViktorijaPanovska))
    collector.add((apache_airflow, FOAF.name, Literal("Apache Airflow", lang="en")))

    tableau = NS.Tableau
    collector.add((tableau, RDF.type, PROV.SoftwareAgent))
    collector.add((tableau, RDF.type, PROV.Agent))
    collector.add((tableau, PROV.actedOnBehalfOf, NS.ViktorijaPanovska))
    collector.add((tableau, FOAF.name, Literal("Tableau", lang="en"))) 
    
    author = NS.ViktorijaPanovska
    collector.add((author, RDF.type, PROV.Person))
    collector.add((author, RDF.type, PROV.Agent))
    collector.add((author, FOAF.givenName, Literal("Viktorija Panovska", lang="en")))
    collector.add((author, FOAF.mbox, URIRef("mailto:viktorijapanovska137@gmail.com")))

    wikipedia = NS.Wikipedia
    collector.add((wikipedia, RDF.type, PROV.Organization))
    collector.add((wikipedia, RDF.type, PROV.Agent))
    collector.add((wikipedia, FOAF.name, Literal("Wikipedia, The Free Encyclopedia", lang="en")))
    collector.add((wikipedia, FOAF.homepage, Literal("https://en.wikipedia.org/wiki/Main_Page", datatype=XSD.anyURI)))

    czech_statistical_office = NS.CzechStatisticalOffice
    collector.add((czech_statistical_office, RDF.type, PROV.Organization))
    collector.add((czech_statistical_office, RDF.type, PROV.Agent))
    collector.add((czech_statistical_office, FOAF.name, Literal("Czech Statistical Office", lang="en")))
    collector.add((czech_statistical_office, FOAF.name, Literal("Český statistický úřad", lang="cs")))
    collector.add((czech_statistical_office, FOAF.homepage, Literal("https://www.czso.cz/", datatype=XSD.anyURI)))

    czech_health_ministry = NS.CzechMinistryOfHealth
    collector.add((czech_health_ministry, RDF.type, PROV.Organization))
    collector.add((czech_health_ministry, RDF.type, PROV.Agent))
    collector.add((czech_health_ministry, FOAF.name, Literal("Ministry of Health of the Czech Republic", lang="en")))
    collector.add((czech_health_ministry, FOAF.name, Literal("Ministerstvo zdravotnictví České Republiky", lang="cs")))
    collector.add((czech_health_ministry, FOAF.homepage, Literal("https://mzd.gov.cz/", datatype=XSD.anyURI)))
    
    
def create_activities(collector: Graph) -> None:
    airflow_activity = NS.ApacheAirflowActivity
    collector.add((airflow_activity, RDF.type, PROV.Activity))
    collector.add((airflow_activity, PROV.wasAssociatedWith, NS.ApacheAirflow))
    collector.add((airflow_activity, PROV.used, NS.CovidCasesDataset))
    collector.add((airflow_activity, PROV.used, NS.VaccinationStationsDataset))
    collector.add((airflow_activity, PROV.used, NS.VaccineUsageDataset))
    collector.add((airflow_activity, PROV.used, NS.PopulationDataset))
    collector.add((airflow_activity, PROV.used, NS.RegionsWebpage))
    collector.add((airflow_activity, PROV.used, NS.DistrictsWebpage))
    collector.add((airflow_activity, PROV.used, NS.VaccinesWebpage))

    data_cube_activity = NS.DataCubeActivity
    collector.add((data_cube_activity, RDF.type, PROV.Activity))
    collector.add((data_cube_activity, PROV.wasAssociatedWith, NS.ViktorijaPanovska))
    collector.add((data_cube_activity, PROV.qualifiedAssociation, NS.ProgrammerAssociation))
    collector.add((data_cube_activity, PROV.wasInformedBy, NS.ApacheAirflowActivity))
    collector.add((data_cube_activity, PROV.used, NS.FactVaccineUsage))
    collector.add((data_cube_activity, PROV.used, NS.DimDates))
    collector.add((data_cube_activity, PROV.used, NS.DimVaccinationStations))
    collector.add((data_cube_activity, PROV.used, NS.DimDistricts))
    collector.add((data_cube_activity, PROV.used, NS.DimVaccines))

    programmer_association = NS.ProgrammerAssociation
    collector.add((programmer_association, RDF.type, PROV.Association))
    collector.add((programmer_association, PROV.agent, NS.ViktorijaPanovska))
    collector.add((programmer_association, PROV.hadRole, NS.Programmer))

    programmer = NS.Programmer
    collector.add((programmer, RDF.type, PROV.Role))

    bar_graph_visualization_activity = NS.BarGraphVisualizationActivity
    collector.add((bar_graph_visualization_activity, RDF.type, PROV.Activity))
    collector.add((bar_graph_visualization_activity, PROV.wasAssociatedWith, NS.Tableau))
    collector.add((bar_graph_visualization_activity, PROV.wasInformedBy, NS.ApacheAirflowActivity))
    collector.add((bar_graph_visualization_activity, PROV.used, NS.FactVaccineUsage))
    collector.add((bar_graph_visualization_activity, PROV.used, NS.DimDates))
    collector.add((bar_graph_visualization_activity, PROV.used, NS.DimDistricts))
    collector.add((bar_graph_visualization_activity, PROV.used, NS.DimVaccinationStations))

    scatterplot_visualization_activity = NS.ScatterplotVisualizationActivity
    collector.add((scatterplot_visualization_activity, RDF.type, PROV.Activity))
    collector.add((scatterplot_visualization_activity, PROV.wasAssociatedWith, NS.Tableau))
    collector.add((scatterplot_visualization_activity, PROV.wasInformedBy, NS.ApacheAirflowActivity))
    collector.add((scatterplot_visualization_activity, PROV.used, NS.FactCovidCases))
    collector.add((scatterplot_visualization_activity, PROV.used, NS.DimDistricts))
    

if __name__ == "__main__":
    try:
        prov_data = create_prov_data()
        if prov_data is not None:
            prov_data.serialize(format='turtle', destination='provenance_document.ttl')
            logging.info('Provenance document serialization successful.')
    except Exception as e:
        logging.error(f'An error occurred: {e}')