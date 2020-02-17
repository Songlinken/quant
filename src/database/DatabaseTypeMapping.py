import psycopg2


def get_database_to_dataframe_type_map(additional_type_mapping=None):
    """
    Map the data type from database to the right data type in pandas dataframe.
    """
    psycopg2_type_tuples = [
        (psycopg2.extensions.FLOAT, 'float64'),
        (psycopg2.extensions.DECIMAL, 'float64'),
        (psycopg2.extensions.DATE, 'datetime64[ns]'),
        (psycopg2.extensions.PYDATETIME, 'datetime64[ns]'),
        (psycopg2.extensions.PYDATETIMETZ, 'datetime64[ns]'),
        (psycopg2.extensions.INTEGER, 'int64'),
        (psycopg2.extensions.LONGINTEGER, 'int64'),
        (psycopg2.extensions.BOOLEAN, 'bool'),
        (psycopg2.extensions.INTERVAL, 'timedelta64[ns]')
    ]

    type_mapping = {}

    for psycopg2_type, pandas_type in psycopg2_type_tuples:
        for type_code in psycopg2_type.values:
            type_mapping[type_code] = pandas_type

    if additional_type_mapping:
        for key, value in additional_type_mapping.item():
            type_mapping[key] = value

    return type_mapping
