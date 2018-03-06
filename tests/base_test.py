import unittest
from itertools import chain
import testing.postgresql
import datetime
from unittest import TestCase


# def convert2datetime(obj):
#     if isinstance(obj, datetime.datetime):
#         return obj
#     elif isinstance(obj, datetime.date):
#         return datetime.datetime.combine(obj, datetime.time.min)
#     else:
#         raise TypeError('cannot convert type {} to datetime'.format(type(obj)))


class BaseTestCasePG(TestCase):
    """ Base class for postgres tests.
         In setUpClass, creates a temporary stand-alone database cls.db,
         In tearDown, it closes all extraneous connections and deletes all circuits in a cascade
         In tearDownClass, removes the temporary database """
    MIGRATIONS = ['CREATE SCHEMA IF NOT EXISTS public']

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.db = testing.postgresql.Postgresql()
        cls._conn, cls._cur = PG.open_connections(cls.db.url())
        for sql in cls.MIGRATIONS:
            cls._cur(sql)

    @classmethod
    def tearDownClass(cls):
        PG.close_connections(cls._conn, cls._cur)
        cls.db.stop()

    def tearDown(self):
        self.close_other_connections()
        self.drop_all()

    def close_other_connections(self):
        self._cur.execute('''
                    SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pid <> pg_backend_pid()''')

    def drop_all(self):
        self._cur.execute('''
           CREATE OR REPLACE FUNCTION public.drop_all()
           RETURNS VOID  AS
           $$
           DECLARE rec RECORD;
           BEGIN
               -- Get all the schemas
                FOR rec IN
                select distinct schemaname
                 from pg_catalog.pg_tables
                 -- You can exclude the schema which you don't want to drop by adding another condition here
                 where schemaname not like 'pg_catalog'
                   LOOP
                     EXECUTE 'DROP SCHEMA ' || rec.schemaname || ' CASCADE';
                   END LOOP;
                   RETURN;
           END;
           $$ LANGUAGE plpgsql;

           select public.drop_all();''')
        self._conn.commit()
