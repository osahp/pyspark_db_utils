import psycopg2
import psycopg2.extras
import logging

logger = logging.getLogger('pg')


class PG:
    """ Postgres context manager """
    @staticmethod
    def open_connections(dsn=None, host=None, dbname=None, user=None, password=None, port=5432,
                         dict_cur=False,
                         real_dict_cur=False):
        """ open connections
        :param: real_dict_cur: if set creates RealDictCursor cursor
        :param: dict_cur: if set creates DictCursor cursor
        :return: connection, cursor """
        assert not (real_dict_cur and dict_cur), 'not (real_dict_cur and dict_cur)'
        if dsn is None:
            if password:
                dsn = "host={} dbname={} user={} password={} port={}".format(
                    host, dbname, user, password, port)
            else:
                dsn = "host={} dbname={} user={} port={}".format(
                    host, dbname, user, port)
        try:
            logger.info('get connection')
            conn = psycopg2.connect(dsn)
            conn.set_client_encoding("UTF-8")
            if dict_cur:
                cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            elif real_dict_cur:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            else:
                cur = conn.cursor()
        except:
            logger.error('failed to connect', exc_info=1,
                         extra={'code': 'db_connection_error'})
            raise
        return conn, cur

    @staticmethod
    def close_connections(conn, cur):
        logger.info('close connection')
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __enter__(self):
        self.conn, self.curr = self.open_connections(*self.args, **self.kwargs)
        return self.conn, self.curr

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connections(self.conn, self.curr)
