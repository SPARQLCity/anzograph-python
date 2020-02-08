#!/usr/bin/env python3

from http.client  import HTTPConnection
from urllib.parse import urlencode
import pandas as pd
import numpy as np
import json
from datetime import datetime, date, time

# Runs SPARQL query at SPARQL endpoint and
# returns results as Python 'dict'
# (for SPARQL1.1 results format refer: https://www.w3.org/TR/sparql11-results-json)
#       sparql_endpoint - ex: 'localhost:7070' or 'localhost' or '10.102.0.5:7070'
#       sparql_query - 'select (count(*) as ?c) {?s?p?o}'
def run_query(sparql_endpoint,sparql_query):
   # create HTTP connection to SPARQL endpoint
   conn = HTTPConnection(sparql_endpoint,timeout=100) #may throw HTTPConnection exception
   # urlencode query for sending
   docbody = urlencode({'query':sparql_query})
   # request result in json
   hdrs = {'Accept': 'application/sparql-results+json',
           'Content-type': 'application/x-www-form-urlencoded'}
   # send post request
   conn.request('POST','/sparql',docbody,hdrs) #may throw exception

   # read response
   resp = conn.getresponse()
   if 200 != resp.status:
      errmsg = resp.read()
      conn.close()
      raise Exception('Query Error',errmsg)  # query processing errors - syntax errors, etc.

   # content-type header, and actual response data
   ctype = resp.getheader('content-type','text/html').lower()
   result = resp.read().lstrip()
   conn.close()

   # check response content-type header
   if ctype.find('json') < 0:
      return result      # not a SELECT?

   # convert result in JSON string into python dict
   return json.loads(result)

# Creates Pandas DataFrame from the results of running
# running SPARQL query at sparql endpoint
def create_dataframe(sparql_endpoint,sparql_query):
   # run query
   result = run_query(sparql_endpoint,sparql_query)  # may throw exception
   # result is in SPARQL results format refer: https://www.w3.org/TR/sparql11-results-json/
   cols = result.get('head',{}).get('vars',[])
   rows = result.get('results',{}).get('bindings',[])

   # extract types and columnar data for rows
   coltype = {}
   nptype = {}
   coldata = {}
   for col in cols:
      coltype[col] = None
      coldata[col] = []
      nptype[col] = None

   # for all rows, save (columnar) data in coldata[] for each col
   for rx in range(len(rows)):
      row = rows[rx]
      for col in cols:
         cell = row.get(col,None)
         if cell is None:  # unbound value
            val = None
            if coltype[col] in ('byte','short','int','integer','float','double','decimal'):
               val = np.nan #missing numeric values as NaN
            coldata[col].append(val)
            continue
         # compute type and datum
         pdval = cell.get('value','')
         vtype = cell.get('type','')
         langtag = cell.get('xml:lang','')
         typeuri = cell.get('datatype','')
         pdtype = 'object'
         if vtype == 'bnode':
            pdval = '_:'+pdval
            coltype[col] = 'object'
         elif langtag != '':
            pdval = '"'+pdval+'"'+'@'+langtag
            coltype[col] = 'object'
         elif typeuri != '':
            #vtype in ('typed-literal')
            typeuri = typeuri.replace('http://www.w3.org/2001/XMLSchema#','')
            coltype[col] = typeuri if (coltype[col] is None or coltype[col] == typeuri) else 'object'
            pdtype,pdval = typed_value(typeuri,pdval)
         nptype[col] = pdtype if (coltype[col] != 'object') else 'object'
         coldata[col].append(pdval) # columnar data
   # instantiate DataFrame
   npdata = {}
   for col in cols:
      npdata[col] = np.array(coldata[col],dtype=np.dtype(nptype[col]))
   return pd.DataFrame(columns=cols,data=npdata)

# util: convert literal val into typed-value based on the typeuri
def typed_value(typeuri,val):
   # {"duration", ColTypeDuration},
   if typeuri in ('boolean'):
      return np.bool, 'true' == val
   elif typeuri in ('byte'):
      return np.byte, np.int8(val)
   elif typeuri in ('short'):
      return np.short, np.short(val)
   elif typeuri in ('integer','int','nonNegativeInteger'):
      return np.intc, np.int(val)
   elif typeuri in ('long'):
      return np.int_, np.int_(val)
   elif typeuri in ('float'):
      return np.single, np.float32(val)
   elif typeuri in ('double', 'decimal'):
      return np.double, np.float64(val)
   elif typeuri in ('dateTime'):
      return np.datetime64, datetime.fromisoformat(val)
   elif typeuri in ('date'):
      return pd.date, date.fromisoformat(val)
   elif typeuri in ('time'):
      return pd.time, time.fromisoformat(val)
   return 'object', val

# example usage
import sys
# Usage: azg3.py 10.102.20.55:7070 qryfile.rq
if __name__ == '__main__':
   if len(sys.argv) >= 3:
      try:
         with open(sys.argv[2],'r') as infp:
            qrystr = infp.read()
            df = create_dataframe(sys.argv[1],qrystr)
            print(df)
            sys.exit(0)
      except Exception as ex:
         print(ex)
   print('Usage:',sys.argv[0],'<server:port>','<filename>')
