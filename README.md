# anzograph- python client modules
Python modules to connect to AnzoGraph

There are two API functions in this module.

1. azg3.run_query(sparql_endpoint, query_string) ==>
    Runs SPARQL query 'query_string' at SPARQL endpoint host 'sparql_endpoint',
    and returns results as a python dictionary map
    
2. azg3.create_dataframe(sparql_endpoint, query_string) ==>
    Runs SPARQL query 'query_string' at SPARQL endpoint host 'sparql_endpoint',
    and returns a Pandas dataframe object
    

