import logging
import threading
import time
from SemanticAgent import SemanticAgent
from DatabaseAgent import DatabaseAgent
from ThreadedWorker import ThreadedWorker

def main():
    dbconfig = {
        'dbname': 'lob',
        'dbhost' :'localhost',
        'dbuser' : 'root',
        'dbpassword' : 'root'
    }

    semantic_config = {
        'uri' : 'https://dbpedia.org/sparql'
    }

    LOGLEVEL = 'DEBUG'

    logger = logging.getLogger()
    formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
    logger.setLevel(LOGLEVEL)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)

    da = DatabaseAgent(**dbconfig)
    sa = SemanticAgent(**semantic_config)

    sqlquery = 'SELECT id, website FROM brewery where dbpedia_uri IS NULL'

    result = da.query(sqlquery)
    da.close()

    threads = []

    # Create new threads
    for item in result:
        while threading.active_count() > 100:
            time.sleep(5)
        t = ThreadedWorker(sa, item, **dbconfig)
        t.start()
        threads.append(t)

    # Wait for all threads to complete
    for t in threads:
        t.join()

if __name__ == '__main__':
    main()