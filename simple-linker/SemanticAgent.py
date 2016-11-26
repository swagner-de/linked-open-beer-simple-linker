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



