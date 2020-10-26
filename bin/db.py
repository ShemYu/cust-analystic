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
        try:
            self.db = MySQLdb.connect(
                host=self.default['host'], user=self.default['user'], passwd=self.default['passwd'], db=self.default['db'], charset=self.default['charset'])
            self.cursor = self.db.cursor()
        except:
            self.db = ''
            self.cursor = ''

    def query(self,sql,com=False):
        """
        input : sql
        output : tuple data
        """
        if self.db and self.cursor:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            if com:
                self.db.commit()
            # print(type(results))
            return results
        else:
            print('Please set_config with section_name first!')
            return ''
        
    def idu(self,sql):
        self.cursor.execute(sql)
        self.db.commit()

    def close(self):
        self.db.close()

    def set_config(self, section_name=''):
        if section_name and section_name in self.config:
            self.default = self.config[section_name]
            self.db = MySQLdb.connect(
                host=self.default['host'], user=self.default['user'], passwd=self.default['passwd'], db=self.default['db'], charset=self.default['charset'])
            self.cursor = self.db.cursor()
        else:
            print('Please check the section name!')
