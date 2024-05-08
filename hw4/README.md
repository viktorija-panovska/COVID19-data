## SYSTEM REQUIREMENTS

- Python (version 3.5 or newer)


## INSTALLATION INSTRUCTIONS

1. Create a Python virtual environment in the current directory.
2. Install the "requirements.txt" in the virtual environment.
3. Execute: `python create_provenance_document.py`


## DESCRIPTION OF SCRIPT FILES

The `create_provenance_document.py` script creates a provenance document describing the ETL workflow implemented by Apache Airflow from Homework Assignment 2, the data cube from Homework Assignment 3, and the two visualizations created with Tableau from Homework Assignment 1. For dimension and fact tables, the primary sources are only the datasets from the Czech Statistical Office and the Ministry of Health of the Czech Republic (so for them I used the property `prov:hadPrimarySource`), while the data coming from Wikipedia and the data derived from other tables is not from a primary source (so I used the property `prov:wasDerivedFrom`). For the Qualified terms requirement, I used the properties `prov:qualifiedAssociation` and `prov:hadRole`, and the classes `prov:Association` and `prov:Role` to describe that the author's role in the DataCubeActivity was as a programmer.