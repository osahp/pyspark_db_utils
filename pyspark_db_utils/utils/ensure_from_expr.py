def ensure_from_expr(sql: str) -> str:
    """
    Check that sql is either 'table_name' or '(select * from table) as foo' expression
    Wraps in '({sql}) as foo'.format(sql) elsewhere
    Args:
        sql: sql to ensure

    Returns:
        original sql or wrapped sql

    Examples:
        >>> ensure_from_expr('data.posdata')
        'data.posdata'
        >>> ensure_from_expr(' ( select * from data.posdata ) as foo ')
        ' ( select * from data.posdata ) as foo '
        >>> ensure_from_expr('select * from data.posdata')  # wraps in this case
        '(select * from data.posdata) as foo'

    Warnings:
        it is not really strict error-sensitive check, it's just hack covered all common use cases
    """
    tokens = [t for t in sql.split() if t]
    if len(tokens) == 1:
        # sql is just table_name
        return sql
    elif len(tokens) < 3:
        # its impossible because it could not be correct SQL
        # but... ok, lets leave it on conscience of developer
        return sql
    else:
        # TODO: use regexp
        correct_open_barcket = tokens[0][0] == '('
        correct_close_barcket = tokens[-3][-1] == ')'
        correct_as = tokens[-2].upper() == 'AS'
        if not correct_open_barcket or not correct_close_barcket or not correct_as:
            return '({sql}) as foo'.format(sql=sql)
