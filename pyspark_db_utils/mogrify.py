import json
import datetime
import string


def mogrify(val) -> str:
    """ cast python values to raw-sql correctly and escape if necessary

    Args:
        val: some value

    Returns:
        mogrified value
    """
    if isinstance(val, (tuple, list, dict)):
        return json.dumps(val)  # are you sure?
    elif isinstance(val, str):
        escaped = val.replace("'", "''")
        return "'{}'".format(escaped)
    elif isinstance(val, (int, float)):
        return str(val)
    elif isinstance(val, datetime.datetime):
        return "'{}'::TIMESTAMP".format(val)
    elif isinstance(val, datetime.date):
        return "'{}'::DATE".format(val)
    elif val is None:
        return 'NULL'
    else:
        raise TypeError('unknown type {} for mogrify'.format(type(val)))


class MogrifyFormatter(string.Formatter):
    """ custom formatter to mogrify {}-like formatting strings """
    def get_value(self, key, args, kwargs) -> str:
        row = args[0]
        return mogrify(row[key])


mogrifier = MogrifyFormatter()
