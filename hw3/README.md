## SYSTEM REQUIREMENTS

- Python (version 3.5 or newer)


## INSTALLATION INSTRUCTIONS

1. Create a Python virtual environment in the current directory.
2. Install the "requirements.txt" in the virtual environment.
3. Execute: `python create_data_cube.py <username> <password>`, where the username and password are those for the PostgreSQL database.
4. To check if the resultant data cube is well-formed, execute: `python check_well_formed.py data_cube.ttl` after running the `create_data_cube` script.


## DESCRIPTION OF SCRIPT FILES

- `create_data_cube.py` - the script takes as input the username and password of the PostgreSQL database, which are used to connect to the Postgres server with host `webik.ms.mff.cuni.cz`, port `5432`, and database `ndbi046`. The data cube constructed in this assignment corresponds to the `fact_vaccine_usage` fact table in the database. The script loads the fact table from the PostgreSQL database, then constructs the data cube from it. The data cube has dimensions 'date', 'district', 'station', and 'vaccine', each of which has an associalted concept and codelist. The data cube has measures 'used_ampules', 'spoiled_ampules', 'administeres_doses', and 'invalid_doses'. The slice defined includes the observations taken on 01.01.2022 about the Comirnaty (Pfizer) vaccines given in Prague (date="01.01.2022", district="Hlavní město Praha", vaccine="Comirnaty"). The output is a `data_cube.ttl` file containing the RDF vocabulary of the data cube.

- `check_well_formed.py` - the script takes the name of the file containing the RDF vocabulary of the data cube and it validates it against the RDF data cube Integrity Constraints. If the RDF dataset passes all the constraints, the resulting output is 'PASSED', and if the RDF dataset fails any constraint, the resulting output is 'FAILED' along with information on which constraint has failed.