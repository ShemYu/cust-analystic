import os
import sys
import MySQLdb
from sqlalchemy import create_engine
import configparser

class db_connector(object):
    """docstring for db_connector."""

    def __init__(self):
        super(db_connector, self).__init__()
        self.config = configparser.ConfigParser()
        self.config.read('doc/db_config.ini')
        self.default = self.config['DEFAULT']
        self.db = MySQLdb.connect(
            host=self.default['host'], user=self.default['user'], passwd=self.default['passwd'], db=self.default['db'], charset=self.default['charset'])
        self.cursor = self.db.cursor()

    def query(self,sql,com=False):
        """
        input : sql
        output : tuple data
        """
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        if com:
            self.db.commit()
        # print(type(results))
        return results
        
    def idu(self,sql):
        self.cursor.execute(sql)
        self.db.commit()

    def close(self):
        self.db.close()
