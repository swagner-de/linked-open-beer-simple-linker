import logging
import threading
import time
import Levenshtein
from tld import get_tld, exceptions
from SemanticAgent import SemanticAgent
from DatabaseAgent import DatabaseAgent

def main():
  semantic_config = {
    'uri': 'https://dbpedia.org/sparql'
  }
  sa = SemanticAgent(**semantic_config)

  dbconfig = {
    'dbname': 'lob',
    'dbhost': 'localhost',
    'dbuser': 'root',
    'dbpassword': 'root'
  }
  LOGLEVEL = 'WARNING'
  logger = logging.getLogger()
  formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
  logger.setLevel(LOGLEVEL)
  streamHandler = logging.StreamHandler()
  streamHandler.setFormatter(formatter)
  logger.addHandler(streamHandler)

  da = DatabaseAgent(**dbconfig)

  result = sa.getAllCompanies(9000)

  processed = []

  for item in result:
      new = {
        'uri' : item['company']['value'],
        'website': item['website']['value']
      }
      try:
        new['domain'] = get_tld(new['website']).split('.')[0]
      except:
        pass
      da.persistDict('companies', [new])
      processed.append(new)

  length = len(processed)
  chunksize = 2000
  """
  for x in range(0, int(length/chunksize)+1):
    da.persistDict('companies', processed[x*chunksize:((x+1)*chunksize)-1 if ((x+1)*chunksize)-1 < length else length])
  """
  da.close()







if __name__ == '__main__':
  main()
