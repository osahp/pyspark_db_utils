from unittest import TestCase
from pyspark_db_utils.utils.ensure_from_expr import ensure_from_expr


class TestEnsureFromExpr(TestCase):
    def test_table_name(self):
        sql = 'data.posdata'
        self.assertEqual(ensure_from_expr(sql), sql)

    def test_select_as(self):
        sql = '(select * from data.posdata) as foo'
        self.assertEqual(ensure_from_expr(sql), sql)

    def test_select_as_spaces(self):
        sql = ' ( select * from data.posdata ) as foo '
        self.assertEqual(ensure_from_expr(sql), sql)

    def test_select_as_multiline(self):
        sql = '''(
            select * from data.posdata
        ) as foo '''
        self.assertEqual(ensure_from_expr(sql), sql)

    def test_select(self):
        sql = '''select * from data.posdata'''
        sql_as = '''({sql}) as foo'''.format(sql=sql)
        self.assertEqual(ensure_from_expr(sql), sql_as)
