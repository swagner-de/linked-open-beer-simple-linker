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

  result = sa.getAllCities(9000)


  for item in result:
      new = {
        'city' : item['city']['value'],
        'label': item['label']['value'].replace('@en',''),
        'postalcode' : item['postalcode']['value'],
        'countrylabel' : item['countrylabel']['value'].replace('@en','')
      }
      da.persistDict('cities', [new])


  da.close()







if __name__ == '__main__':
  main()
