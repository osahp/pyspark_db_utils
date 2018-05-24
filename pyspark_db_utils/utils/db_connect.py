from contextlib import contextmanager

import jaydebeapi

from pyspark_db_utils.get_jars import get_jars


@contextmanager
def db_connect(con_info: dict, autocommit: bool=False):
    """ context manager, opens and closes connection correctly

    Args:
        con_info: con_info for db
        autocommit: if False (default), disable autocommit mode
                    if True, enable autocommit mode

    Yields:
        connection, cursor
    """
    conn = jaydebeapi.connect(jclassname=con_info['driver'],
                              url='jdbc:{dbtype}://{host}/{dbname}'.format(**con_info),
                              driver_args={k: con_info[k] for k in ('user', 'password')},
                              jars=get_jars()  # jaydebeapi create java-process once, so add all possible drivers
                              )
    conn.jconn.setAutoCommit(autocommit)
    curs = conn.cursor()
    yield conn, curs
    curs.close()
    conn.close()
