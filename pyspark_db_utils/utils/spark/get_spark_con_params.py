def get_spark_con_params(con_info):
    """ get DB jdbc connection string from config `con_info` """
    return dict(url='jdbc:{dbtype}://{host}/{dbname}'.format(**con_info),
                properties={k: con_info[k]
                            for k in ('user', 'password', 'driver')})
