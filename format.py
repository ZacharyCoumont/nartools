from psycopg.rows import dict_row

from util import get_table

prov_code_map = {
    10: 'NL',
    11: 'PE',
    12: 'NS',
    13: 'NB',
    24: 'QC',
    35: 'ON',
    46: 'MB',
    47: 'SK',
    48: 'AB',
    59: 'BC',
    60: 'YT',
    61: 'NT',
    62: 'NU',
}

def format_address_base(conn, addr_guid):
    table = get_table()

    with conn.cursor(row_factory=dict_row) as cur:
        address = cur.execute("SELECT * FROM " + table.get_sql() + " WHERE addr_guid = %s LIMIT 1", (addr_guid,)).fetchone()
        if address is not None:
            return address

        locations = cur.execute("SELECT * FROM " + table.get_sql() + " WHERE loc_guid = %s", (addr_guid,)).fetchall()
        if len(locations) == 0:
            return None

        location_composite = locations[0]
        for loc in locations[1:]:
            for k, v in loc.items():
                if location_composite[k] is not None and location_composite[k] != v:
                    location_composite[k] = None

        return location_composite

def format_civic_address(conn, addr_guid, one_line=False):
    address = format_address_base(conn, addr_guid)
    if address is None:
        return None

    apt_no = address['apt_no_label'] or ''
    civic_no = ' '.join([x for x in [address['civic_no'], address['civic_no_suffix']] if x])
    if apt_no and civic_no:
        civic_no = f"{apt_no}-{civic_no}"
    elif apt_no:
        civic_no = apt_no

    prov_abbr = prov_code_map.get(address['prov_code'], '')

    street_type_before = address['official_street_type'] in ['RUE', 'AV'] or prov_abbr in ['QC']

    line_1 = ' '.join([x for x in [
        civic_no,
        address['official_street_type'] if street_type_before else '',
        address['official_street_name'],
        '' if street_type_before else address['official_street_type'],
        address['official_street_dir'],
    ] if x])

    line_2 = ' '.join([x for x in [
        address['mail_mun_name'],
        prov_abbr,
    ] if x])

    line_3 = ''
    if address['mail_postal_code']:
        line_3 = f"{address['mail_postal_code'][:3]} {address['mail_postal_code'][3:]}"

    return (', ' if one_line else '\n').join([x for x in [line_1, line_2, line_3] if x]).upper()

def format_mailing_address(conn, addr_guid, one_line=False):
    address = format_address_base(conn, addr_guid)
    if address is None:
        return None

    apt_no = address['apt_no_label'] or ''
    civic_no = ' '.join([x for x in [address['civic_no'], address['civic_no_suffix']] if x])
    if apt_no and civic_no:
        civic_no = f"{apt_no}-{civic_no}"
    elif apt_no:
        civic_no = apt_no

    prov_abbr = address['mail_prov_abvn']

    street_type_before = address['mail_street_type'] in ['RUE', 'AV'] or prov_abbr in ['QC']

    line_0 = address['bu_n_civic_add'] or ''

    line_1 = ' '.join([x for x in [
        address['mail_street_type'] if street_type_before else '',
        address['mail_street_name'],
        '' if street_type_before else address['mail_street_type'],
        address['mail_street_dir'],
    ] if x])

    if line_1:
        line_1 = ' '.join([civic_no, line_1]).strip()

    line_2 = ' '.join([x for x in [
        address['mail_mun_name'],
        prov_abbr,
    ] if x])

    line_3 = ''
    if address['mail_postal_code']:
        line_3 = f"{address['mail_postal_code'][:3]} {address['mail_postal_code'][3:]}"

    return (', ' if one_line else '\n').join([x for x in [line_0, line_1, line_2, line_3] if x]).upper()
