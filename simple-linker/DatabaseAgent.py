import MySQLdb
import MySQLdb.cursors
import warnings
import logging
import sys

class DatabaseAgent:
    def __init__(self, **kwargs):
        self.logger = logging.getLogger()
        try:
            self.db = MySQLdb.connect(kwargs['dbhost'], kwargs['dbuser'], kwargs['dbpassword'], kwargs['dbname'],
                                      cursorclass=MySQLdb.cursors.DictCursor, charset='utf8')
            warnings.filterwarnings("error", category=MySQLdb.Warning)
            self.cnx = self.db.cursor()
            self.db.autocommit(True)
            self.logger.debug(
                'Connected to database %s on %s with user %s' % (kwargs['dbname'], kwargs['dbhost'], kwargs['dbuser']))
        except MySQLdb.Error as e:
            self.logger.error(
                "Error while establishing connection to the database server [%d]: %s" % (e.args[0], e.args[1]))
            sys.exit(1)


    def execute(self, statement):
        if statement:
            try:
                self.logger.debug('Executing SQL-query:\n\t%s' % statement.replace('\n', '\n\t'))
                self.cnx.execute(statement)
            except MySQLdb.Warning as e:
                self.logger.warn("Warning while executing statement: %s" % e)
            except MySQLdb.Error as e:
                self.logger.error("Error while executing statement [%d]: %s" % (e.args[0], e.args[1]))
                self.close()
                sys.exit(1)

    def query(self, query):
        if query:
            try:
                self.logger.debug('Executing SQL-query:\n\t%s' % query.replace('\n', '\n\t'))
                self.cnx.execute(query)
                return self.cnx.fetchall()

            except MySQLdb.Warning as e:
                self.logger.warn("Warning while executing statement: %s" % e)

            except MySQLdb.Error as e:
                self.logger.error("Error while executing statement [%d]: %s" % (e.args[0], e.args[1]))
                self.close()
                sys.exit(1)


    def close(self):
        self.cnx.close()
        self.db.close()
        self.logger.debug('Closed DB connection')

