## SYSTEM REQUIREMENTS

- Python (version 3.5 or newer)


## INSTALLATION INSTRUCTIONS

1. Create a Python virtual environment in the current directory.
2. Install the "requirements.txt" in the virtual environment.
3. Execute: `python create_data_catalog.py` to create the data catalog file.
4. Once the data catalog file is created, execute `python query_data_catalog.py data_catalog.ttl` to run the queries. 


## DESCRIPTION OF SCRIPT FILES

- `create_data_catalog.py` outputs a file `data_catalog.ttl` containing the data catalog created for the fact_vaccine_usage dataset, the same one that was used to create the data cube. 

- `query_data_catalog.py` takes the name of the data catalog file as input and outputs the results of all the queries to the console. There are two queries: one gets, for every dataset, all the formats it is available in, and the other gets all the creators that have created a dataset in the previous month.
