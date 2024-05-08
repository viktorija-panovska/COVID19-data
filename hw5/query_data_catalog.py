from rdflib import Graph
import logging 
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s") 

QUERIES = [
    # For every dataset, get all the formats it is available in.
    """
        SELECT ?dataset ?format
        WHERE {
            ?dataset a dcat:Dataset ;
                dcat:distribution/dcterms:format ?format .
        }
    """,

    # Get all the creators that have created a dataset in the previous month.
    """
        SELECT ?creator
        WHERE {
            ?dataset a dcat:Dataset ;
                dcterms:creator ?creator ;
                dcterms:issued ?date .

            BIND (IF(MONTH(NOW())=1, 12, MONTH(NOW())-1) as ?lastMonth)
            FILTER(MONTH(?date)=?lastMonth)
        }
    """
]


def main(data_catalog_file: str) -> None:
    g = Graph()
    g.parse(data_catalog_file)

    for index, query in enumerate(QUERIES):
        print(f"---\nRunning query {index+1}...")
        try:
            for row in g.query(query):
                print(row[0] if len(row) == 1 else ', '.join([elem for elem in row]))
        except Exception as ex:
            logging.warning(f"Error occurred while running query {index+1}.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logging.error("Usage: python query_data_catalog.py <data_catalog_file>")
        sys.exit(1)

    main(sys.argv[1])