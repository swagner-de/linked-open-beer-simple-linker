from SPARQLWrapper import SPARQLWrapper, JSON
import logging


class SemanticAgent:

    def __init__(self, **config):
        self.logger = logging.getLogger()
        self.endpoint = SPARQLWrapper(config['uri'])

    def query(self, statement):
        self.endpoint.setQuery(statement)
        self.endpoint.setReturnFormat(JSON)
        return self.endpoint.query().convert()['results']['bindings']

    def getAllCompanies(self, chunksize):
      offset = 0
      values = []
      query = """
          select distinct ?company, ?website WHERE {
                                          ?company a dbo:Company .
                                          ?company dbo:wikiPageExternalLink ?website .}
          LIMIT %s
          OFFSET %s
        """
      while True:

        result = self.query(query % (chunksize, offset))
        if not result:
          return values
        offset += chunksize
        values.extend(result)
        print(str(len(values)) + ' values fetched from dbpedia')


    def getAllCities(self, chunksize):
      offset = 0
      values = []
      query = """
            select distinct ?city, ?label, ?postalcode, ?countrylabel WHERE {
                                            ?city a dbo:City .
                                            ?city rdfs:label ?label .
                                            ?city dbo:postalCode ?postalcode .
                                            ?city dbo:country ?country .
                                            ?country rdfs:label ?countrylabel .
                                            FILTER (lang(?label) = 'en')
                                            FILTER (lang(?countrylabel) = 'en')}
          LIMIT %s
          OFFSET %s
        """
      while True:

        result = self.query(query % (chunksize, offset))
        if not result:
          return values
        offset += chunksize
        values.extend(result)
        print(str(len(values)) + ' values fetched from dbpedia')


