import logging
import mysql.connector
from settings import config


class Mysql:
    def __enter__(self):
        logging.info('Initialize connection to database')
        self.conn = mysql.connector.connect(**config['mysql'])
        logging.info('Connection is ready')
        return self.conn

    def __exit__(self, *exc_info):
        if exc_info[0]:
            logging.warning('Issue with database connection. Rollback')
            self.conn.rollback()
            logging.exception('Exception details:')
        self.conn.close()
