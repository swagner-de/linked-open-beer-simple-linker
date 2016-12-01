import threading
from tld import get_tld
from DatabaseAgent import DatabaseAgent
from urllib import error


class ThreadedWorker(threading.Thread):
    def __init__(self, sa, item, **dbconfig):
        self.dbconfig = dbconfig
        self.sa = sa
        self.item = item
        threading.Thread.__init__(self)

    def run(self):
        da = DatabaseAgent(**self.dbconfig)
        sqlinsert = "UPDATE brewery SET dbpedia_uri = '%s'  WHERE id = %s"
        sparql_query = """ select distinct ?company WHERE {
                                ?company a dbo:Company .
                                ?company dbo:wikiPageExternalLink ?y .
                                FILTER regex(?y, ".*\\\\.%s\\\\..*", "i") }
                            """
        try:
            domain = get_tld(self.item['website']).split('.')[0]
            semantic_result = self.sa.query(sparql_query % (domain))[0]['company']['value']
        except IndexError:
            semantic_result = 'no result on dbpedia'
        except error.HTTPError:
            semantic_result = 'NULL'
        semantic_result = semantic_result.replace("'", "\\'")
        da.execute(sqlinsert % (semantic_result, self.item['id']))
        da.close()
