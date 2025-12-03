import os

from pypika import Table

def get_table():
    table_parts = os.getenv("POSTGRES_NAR_TABLE", "nar_addresses").split('.')

    if len(table_parts) == 2:
        schema_name = table_parts[0]
        table_name = table_parts[1]
    else:
        schema_name = None
        table_name = table_parts[0]

    return Table(table_name, schema=schema_name)
