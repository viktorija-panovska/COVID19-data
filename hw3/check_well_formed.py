# Copyright (c) 2024 Pavel Koupil
# 
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
# 
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.


#!/usr/bin/env python3

from rdflib import Graph
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s") 

class IntegrityConstraints:
    """
    Class for defining and validating integrity constraints on RDF data.
    """

    # IC-1. Unique DataSet
    # Every qb:Observation has exactly one associated qb:DataSet.
    IC1 = """
    ASK {
      {
        # Check observation has a data set
        ?obs a qb:Observation .
        FILTER NOT EXISTS { ?obs qb:dataSet ?dataset1 . }
      } UNION {
        # Check has just one data set
        ?obs a qb:Observation ;
           qb:dataSet ?dataset1, ?dataset2 .
        FILTER (?dataset1 != ?dataset2)
      }
    }
    """

    # IC-2. Unique DSD
    # Every qb:DataSet has exactly one associated qb:DataStructureDefinition.
    IC2 = """
    ASK {
      {
        # Check dataset has a dsd
        ?dataset a qb:DataSet .
        FILTER NOT EXISTS { ?dataset qb:structure ?dsd . }
      } UNION { 
        # Check has just one dsd
        ?dataset a qb:DataSet ;
           qb:structure ?dsd1, ?dsd2 .
        FILTER (?dsd1 != ?dsd2)
      }
    }
    """

    # IC-3. DSD includes measure
    # Every qb:DataStructureDefinition must include at least one declared measure.
    IC3 = """
    ASK {
      ?dsd a qb:DataStructureDefinition .
      FILTER NOT EXISTS { ?dsd qb:component [qb:measure [a qb:MeasureProperty]] }
    }
    """

    # IC-4. Dimensions have range
    # Every dimension declared in a qb:DataStructureDefinition must have a declared rdfs:range.
    IC4 = """
    ASK {
      ?dim a qb:DimensionProperty .
      FILTER NOT EXISTS { ?dim rdfs:range [] }
    }
    """

    # IC-5. Concept dimensions have code lists
    # Every dimension with range skos:Concept must have a qb:codeList.
    IC5 = """
    ASK {
      ?dim a qb:DimensionProperty ;
           rdfs:range skos:Concept .
      FILTER NOT EXISTS { ?dim qb:codeList [] }
    }
    """

    # IC-6. Only attributes may be optional
    # The only components of a qb:DataStructureDefinition that may be marked as optional, using qb:componentRequired are attributes.
    IC6 = """
    ASK {
      ?dsd qb:component ?componentSpec .
      ?componentSpec qb:componentRequired "false"^^xsd:boolean ;
                     qb:componentProperty ?component .
      FILTER NOT EXISTS { ?component a qb:AttributeProperty }
    }
    """

    # IC-7. Slice Keys must be declared
    # Every qb:SliceKey must be associated with a qb:DataStructureDefinition.
    IC7 = """
    ASK {
        ?sliceKey a qb:SliceKey .
        ?dataStructureDefinition a qb:DataStructureDefinition .
        FILTER NOT EXISTS { ?dataStructureDefinition qb:sliceKey ?sliceKey }
    }
    """

    # IC-8. Slice Keys consistent with DSD
    # Every qb:componentProperty on a qb:SliceKey must also be declared as a qb:component of the associated qb:DataStructureDefinition.
    IC8 = """
    ASK {
      ?slicekey a qb:SliceKey;
          qb:componentProperty ?prop .
      ?dsd qb:sliceKey ?slicekey .
      FILTER NOT EXISTS { ?dsd qb:component [(qb:componentProperty | qb:dimension) ?prop] }
    }
    """

    # IC-9. Unique slice structure
    # Each qb:Slice must have exactly one associated qb:sliceStructure.
    IC9 = """
    ASK {
      {
        # Slice has a key
        ?slice a qb:Slice .
        FILTER NOT EXISTS { ?slice qb:sliceStructure ?key }
      } UNION {
        # Slice has just one key
        ?slice a qb:Slice ;
               qb:sliceStructure ?key1, ?key2;
        FILTER (?key1 != ?key2)
      }
    }
    """

    # IC-10. Slice dimensions complete
    # Every qb:Slice must have a value for every dimension declared in its qb:sliceStructure.
    IC10 = """
    ASK {
      ?slice qb:sliceStructure [qb:componentProperty ?dim] .
      FILTER NOT EXISTS { ?slice ?dim [] }
    }
    """

    # IC-11. All dimensions required
    # Every qb:Observation has a value for each dimension declared in its associated qb:DataStructureDefinition.
    IC11 = """
    ASK {
        ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .
        ?dim a qb:DimensionProperty;
        FILTER NOT EXISTS { ?obs ?dim [] }
    }
    """

    # IC-12. No duplicate observations
    # No two qb:Observations in the same qb:DataSet may have the same value for all dimensions.
    IC12 = """
    ASK {
      FILTER( ?allEqual )
      {
        # For each pair of observations test if all the dimension values are the same
        SELECT (MIN(?equal) AS ?allEqual) WHERE {
            ?obs1 qb:dataSet ?dataset .
            ?obs2 qb:dataSet ?dataset .
            FILTER (?obs1 != ?obs2)
            ?dataset qb:structure/qb:component/qb:componentProperty ?dim .
            ?dim a qb:DimensionProperty .
            ?obs1 ?dim ?value1 .
            ?obs2 ?dim ?value2 .
            BIND( ?value1 = ?value2 AS ?equal)
        } GROUP BY ?obs1 ?obs2
      }
    }
    """

    # IC-13. Required attributes
    # Every qb:Observation has a value for each declared attribute that is marked as required.
    IC13 = """
    ASK {
        ?obs qb:dataSet/qb:structure/qb:component ?component .
        ?component qb:componentRequired "true"^^xsd:boolean ;
                   qb:componentProperty ?attr .
        FILTER NOT EXISTS { ?obs ?attr [] }
    }
    """

    # IC-14. All measures present
    # In a qb:DataSet which does not use a Measure dimension then each individual qb:Observation must have a value for every declared measure.
    IC14 = """
    ASK {
        # Observation in a non-measureType cube
        ?obs qb:dataSet/qb:structure ?dsd .
        FILTER NOT EXISTS { ?dsd qb:component/qb:componentProperty qb:measureType }

        # verify every measure is present
        ?dsd qb:component/qb:componentProperty ?measure .
        ?measure a qb:MeasureProperty;
        FILTER NOT EXISTS { ?obs ?measure [] }
    }
    """

    # IC-15. Measure dimension consistent
    # In a qb:DataSet which uses a Measure dimension then each qb:Observation must have a value for the measure corresponding to its given qb:measureType.
    IC15 = """
    ASK {
        # Observation in a measureType-cube
        ?obs qb:dataSet/qb:structure ?dsd ;
             qb:measureType ?measure .
        ?dsd qb:component/qb:componentProperty qb:measureType .
        # Must have value for its measureType
        FILTER NOT EXISTS { ?obs ?measure [] }
    }
    """

    # IC-16. Single measure on measure dimension observation
    # In a qb:DataSet which uses a Measure dimension then each qb:Observation must only have a value for one measure (by IC-15 this will be the measure corresponding to its qb:measureType).
    IC16 = """
    ASK {
        # Observation with measureType
        ?obs qb:dataSet/qb:structure ?dsd ;
             qb:measureType ?measure ;
             ?omeasure [] .
        # Any measure on the observation
        ?dsd qb:component/qb:componentProperty qb:measureType ;
             qb:component/qb:componentProperty ?omeasure .
        ?omeasure a qb:MeasureProperty .
        # Must be the same as the measureType
        FILTER (?omeasure != ?measure)
    }
    """

    # IC-17. All measures present in measures dimension cube
    # In a qb:DataSet which uses a Measure dimension then if there is a Observation for some combination of non-measure dimensions then there must be other Observations with the same non-measure dimension values for each of the declared measures.
    IC17 = """
    ASK {
      {
          # Count number of other measures found at each point 
          SELECT ?numMeasures (COUNT(?obs2) AS ?count) WHERE {
              {
                  # Find the DSDs and check how many measures they have
                  SELECT ?dsd (COUNT(?m) AS ?numMeasures) WHERE {
                      ?dsd qb:component/qb:componentProperty ?m.
                      ?m a qb:MeasureProperty .
                  } GROUP BY ?dsd
              }

              # Observation in measureType cube
              ?obs1 qb:dataSet/qb:structure ?dsd;
                    qb:dataSet ?dataset ;
                    qb:measureType ?m1 .

              # Other observation at same dimension value
              ?obs2 qb:dataSet ?dataset ;
                    qb:measureType ?m2 .
              FILTER NOT EXISTS { 
                  ?dsd qb:component/qb:componentProperty ?dim .
                  FILTER (?dim != qb:measureType)
                  ?dim a qb:DimensionProperty .
                  ?obs1 ?dim ?v1 . 
                  ?obs2 ?dim ?v2. 
                  FILTER (?v1 != ?v2)
              }

          } GROUP BY ?obs1 ?numMeasures
            HAVING (?count != ?numMeasures)
      }
    }
    """

    # IC-18. Consistent data set links
    # If a qb:DataSet D has a qb:slice S, and S has an qb:observation O, then the qb:dataSet corresponding to O must be D.
    IC18 = """
    ASK {
        ?dataset qb:slice       ?slice .
        ?slice   qb:observation ?obs .
        FILTER NOT EXISTS { ?obs qb:dataSet ?dataset . }
    }
    """

    # IC-19. Codes from code list
    # If a dimension property has a qb:codeList, then the value of the dimension property on every qb:Observation must be in the code list.
    IC19 = """
    ASK {
        ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .
        ?dim a qb:DimensionProperty ;
            qb:codeList ?list .
        ?list a skos:ConceptScheme .
        ?obs ?dim ?v .
        FILTER NOT EXISTS { ?v a skos:Concept ; skos:inScheme ?list }
    }
    """

    IC19_B = """
    ASK {
        ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .
        ?dim a qb:DimensionProperty ;
            qb:codeList ?list .
        ?list a skos:Collection .
        ?obs ?dim ?v .
        FILTER NOT EXISTS { ?v a skos:Concept . ?list skos:member+ ?v }
    }
    """

    # IC-20. Codes from hierarchy
    # If a dimension property has a qb:HierarchicalCodeList with a non-blank qb:parentChildProperty then the value of that dimension property on every qb:Observation must be reachable from a root of the hierarchy using zero or more hops along the qb:parentChildProperty links.
    IC20 = """
    ASK {
        ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .
        ?dim a qb:DimensionProperty ;
            qb:codeList ?list .
        ?list a qb:HierarchicalCodeList .
        ?obs ?dim ?v .
        FILTER NOT EXISTS { ?list qb:hierarchyRoot/<$p>* ?v }
    }
    """

    # IC-21. Codes from hierarchy (inverse)
    # If a dimension property has a qb:HierarchicalCodeList with an inverse qb:parentChildProperty then the value of that dimension property on every qb:Observation must be reachable from a root of the hierarchy using zero or more hops along the inverse qb:parentChildProperty links.
    IC21 = """
    ASK {
        ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .
        ?dim a qb:DimensionProperty ;
             qb:codeList ?list .
        ?list a qb:HierarchicalCodeList .
        ?obs ?dim ?v .
        FILTER NOT EXISTS { ?list qb:hierarchyRoot/(^<$p>)* ?v }
    }
    """

    @staticmethod
    def validate_dataset(rdf_file_source: str) -> bool:
        """
        Validates the RDF dataset against integrity constraints.

        Args:
            rdf_file_source (str): The file path to the RDF dataset.

        Returns:
            bool: True if the dataset passes all integrity constraints, False otherwise.
        """
        CONSTRAINTS = [IntegrityConstraints.IC1, IntegrityConstraints.IC2, IntegrityConstraints.IC3, IntegrityConstraints.IC4, IntegrityConstraints.IC5, IntegrityConstraints.IC6, IntegrityConstraints.IC7, IntegrityConstraints.IC8, IntegrityConstraints.IC9, IntegrityConstraints.IC10,
                       IntegrityConstraints.IC11, IntegrityConstraints.IC12, IntegrityConstraints.IC13, IntegrityConstraints.IC14, IntegrityConstraints.IC15, IntegrityConstraints.IC16, IntegrityConstraints.IC17, IntegrityConstraints.IC18, IntegrityConstraints.IC19, IntegrityConstraints.IC19_B, IntegrityConstraints.IC20,
                       IntegrityConstraints.IC21 ] 

        g = Graph()
        g.parse(rdf_file_source)
        
        result = False
        for index, constraint in enumerate(CONSTRAINTS):
            try:
                for row in g.query(constraint):
                    if row:
                        logging.warning(f"Failed IC{index+1} checking.")
                        result = row
            except Exception as ex:
                logging.warning(f"Error occurred while IC{index+1} checking.")

        return result


def main(data_cube_file: str) -> None:
    """
    Main function to execute integrity constraints validation.
    """
    print("Validating " + data_cube_file)
    logging.info("FAILED" if IntegrityConstraints.validate_dataset(data_cube_file) else "PASSED")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logging.error("Usage: python script.py <data_cube_file>")
        sys.exit(1)

    main(sys.argv[1])
