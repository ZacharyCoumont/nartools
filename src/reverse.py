from psycopg.rows import dict_row

from .util import get_table

def reverse_geocode(conn, latitude, longitude):
    table = get_table()

    with conn.cursor(row_factory=dict_row) as cur:
        results = cur.execute("SELECT addr_guid, loc_guid, ST_Distance(ST_Point(bg_x, bg_y, 3347), ST_Transform(ST_Point(%s, %s, 4326), 3347)) AS distance FROM " + table.get_sql() + " ORDER BY distance LIMIT 4", (longitude, latitude)).fetchall()

        first_result = results[0]
        unique_locations = set([row['loc_guid'] for row in results])

        if len(unique_locations) == 1:
            return (first_result['loc_guid'], first_result['distance'])
        else:
            return (first_result['addr_guid'], first_result['distance'])
