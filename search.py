import re
import sys

from canaddress import canaddress
from unidecode import unidecode
from Levenshtein import distance
from pypika import Query, Criterion, functions as fn

from util import get_table

street_type_mapping = {
    "STREET": "ST",
    "ROAD": "RD",
    "AVENUE": "AVE",
    "BOULEVARD": "BLVD",
    "COURT": "CRT",
    "CRESENT": "CRES",
    "CRESCENT": "CRES",
}

street_dir_mapping = {
    "NORTH": "N",
    "SOUTH": "S",
    "EAST": "E",
    "WEST": "W",
    "NORTHEAST": "NE",
    "NORTHWEST": "NW",
    "SOUTHEAST": "SE",
    "SOUTHWEST": "SW",
    "NORTH EAST": "NE",
    "NORTH WEST": "NW",
    "SOUTH EAST": "SE",

    "NORD": "N",
    "SUD": "S",
    "EST": "E",
    "OUEST": "O",
    "NORDEST": "NE",
    "NORDOUEST": "NO",
    "SUDEST": "SE",
    "SUDOUEST": "SO",
    "NORD EST": "NE",
    "NORD OUEST": "NO",
    "SUD EST": "SE",
    "SUD OUEST": "SO",
}

street_ordinal_mapping = {
    "FIRST": "1ST",
    "SECOND": "2ND",
    "THIRD": "3RD",
    "FOURTH": "4TH",
    "FIFTH": "5TH",
    "SIXTH": "6TH",
    "SEVENTH": "7TH",
    "EIGHTH": "8TH",
    "NINTH": "9TH",
    "TENTH": "10TH",
    "ELEVENTH": "11TH",
    "TWELFTH": "12TH",
    "THIRTEENTH": "13TH",
    "FOURTEENTH": "14TH",
    "FIFTEENTH": "15TH",
    "SIXTEENTH": "16TH",
    "SEVENTEENTH": "17TH",
    "EIGHTEENTH": "18TH",
    "NINETEENTH": "19TH",
    "TWENTIETH": "20TH",
    "TWENTYFIRST": "21ST",
    "TWENTY FIRST": "21ST",
}

def normalize_postal_code(value):
    return re.sub(r'[^0-9A-Z]', '', value.upper())

def normalize_value(value):
    if value is None:
        return ''

    value = value.upper()
    value = re.sub(r'\s+', ' ', value)
    value = value.strip()

    return value

def simplify_value(value):
    if value is None:
        return ''

    value = unidecode(value)
    value = normalize_value(value)
    value = re.sub(r"'", '', value)
    value = re.sub(r'-', ' ', value)
    value = re.sub(r'\.', '', value)

    if value in street_ordinal_mapping:
        value = street_ordinal_mapping[value]

    return value

place_cache = []
place_cache_simple = [] # place_cache_simple = [simplify_value(x) for x in place_cache]

def number_to_ordinal(n):
    if 10 <= n % 100 <= 20:
        suffix = 'th'
    else:
        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
    return str(n) + suffix

def find_closest(search_value, options):
    best_options = []
    best_score = 999999

    for option in options:
        return_value = option

        if isinstance(option, tuple):
            return_value = option[1]
            option = option[0]

        if option in best_options:
            continue

        score = distance(search_value, option, score_cutoff=best_score+1)
        if score < best_score:
            best_options = [return_value]
            best_score = score
        elif score == best_score:
            best_options.append(return_value)

        # Street names can still be ambiguous even when they're an imperfect match, for example when we don't know if something is north or south
        # if best_score == 0:
        #     break

    best_error = (best_score / max(len(search_value), 1)) if len(best_options) > 0 else 1

    return (list(set(best_options)) if len(best_options) > 1 else best_options, best_score, best_error)

def yield_street_variations(narrowed):
    for row in narrowed:
        (street_name, street_type, street_dir) = row

        if street_name is None and street_type is None and street_dir is None:
            continue

        # Saskatoon has a bunch of "Avenue X" street_names, and those are gonna be edge cases
        if simplify_value(street_name).startswith('AVENUE') and street_type is None:
            log('FIXING AVENUE X CASE FOR:', street_name)
            street_type = 'AVE'
            street_name = street_name.replace('AVENUE', '').strip()

        # If it's just digits, it could be ordinal in some contexts, so try it both ways
        if street_name is not None and street_name.isdigit():
            street_name_ordinal = number_to_ordinal(int(street_name)).upper()

            yield (' '.join([simplify_value(x) for x in [street_name_ordinal, street_dir] if x]), row)
            yield (' '.join([simplify_value(x) for x in [street_name_ordinal, street_type, street_dir] if x]), row)
            yield (' '.join([simplify_value(x) for x in [street_type, street_name_ordinal, street_dir] if x]), row)
            yield (' '.join([simplify_value(x) for x in [street_type, street_name_ordinal] if x]), row)
            yield (' '.join([simplify_value(x) for x in [street_name_ordinal, street_type] if x]), row)
            yield (' '.join([simplify_value(x) for x in [street_name_ordinal] if x]), row)

        # If it's ordinal, it could be not in some contexts, so try it both ways
        if street_name is not None and re.match(r'^\d+(ST|ND|RD|TH)$', simplify_value(street_name)):
            street_name_non_ordinal = simplify_value(street_name)[:-2]

            yield (' '.join([simplify_value(x) for x in [street_name_non_ordinal, street_dir] if x]), row)
            yield (' '.join([simplify_value(x) for x in [street_name_non_ordinal, street_type, street_dir] if x]), row)
            yield (' '.join([simplify_value(x) for x in [street_type, street_name_non_ordinal, street_dir] if x]), row)
            yield (' '.join([simplify_value(x) for x in [street_type, street_name_non_ordinal] if x]), row)
            yield (' '.join([simplify_value(x) for x in [street_name_non_ordinal, street_type] if x]), row)
            yield (' '.join([simplify_value(x) for x in [street_name_non_ordinal] if x]), row)

        yield (' '.join([simplify_value(x) for x in [street_name, street_dir] if x]), row)
        yield (' '.join([simplify_value(x) for x in [street_name, street_type, street_dir] if x]), row)
        yield (' '.join([simplify_value(x) for x in [street_type, street_name, street_dir] if x]), row)
        yield (' '.join([simplify_value(x) for x in [street_type, street_name] if x]), row)
        yield (' '.join([simplify_value(x) for x in [street_name, street_type] if x]), row)
        yield (' '.join([simplify_value(x) for x in [street_name] if x]), row)

def yield_civic_no_variations(deep_results):
    for row in deep_results:
        (addr_guid, loc_guid, apt_no_label, civic_no, civic_no_suffix) = row

        if civic_no and civic_no_suffix:
            yield (' '.join([simplify_value(x) for x in [apt_no_label, civic_no + civic_no_suffix] if x]), row)
        yield (' '.join([simplify_value(x) for x in [apt_no_label, civic_no, civic_no_suffix] if x]), row)

def log(*args):
    if '--debug' in sys.argv:
        print(*args)

narrow_cache = {}

def execute_log(cur, *args):
    # print('SQL:', args[0] % args[1:])
    return cur.execute(*args)

def find_address(conn, address):
    global place_cache, place_cache_simple, narrow_cache

    table = get_table()

    with conn.cursor() as cur:
        if len(place_cache) == 0:
            print('Populating place cache...')
            municipal_query = Query.from_(table).select(table.mail_mun_name).distinct().where(table.mail_mun_name.isnotnull())
            place_cache = [x[0] for x in execute_log(cur, municipal_query.get_sql()).fetchall()]
            place_cache_simple = [simplify_value(x) for x in place_cache]

        try:
            (tags, specificity) = canaddress.tag(address)
            # log(tags, specificity)
            log(tags)

            street_name_parts = [
                tags.get('StreetNamePreType'),
                tags.get('StreetNamePreDirectional'),
                tags.get('StreetName'),
                tags.get('StreetNamePostType'),
                tags.get('StreetNamePostDirectional'),
            ]
            street_name_parts = [simplify_value(x) for x in street_name_parts]

            if street_name_parts[0] in street_type_mapping:
                street_name_parts[0] = street_type_mapping[street_name_parts[0]]
            if street_name_parts[3] in street_type_mapping:
                street_name_parts[3] = street_type_mapping[street_name_parts[3]]

            if street_name_parts[4] in street_dir_mapping:
                street_name_parts[4] = street_dir_mapping[street_name_parts[4]]

            search_street_name = ' '.join([x for x in street_name_parts if x])

            numbers_parts = [
                tags.get('SubaddressIdentifier'),
                tags.get('AddressNumberPrefix'),
                tags.get('AddressNumber'),
                tags.get('AddressNumberSuffix'),
            ]
            search_numbers = ' '.join([simplify_value(x) for x in numbers_parts if x])

            narrowed = None
            narrow_conditions = []

            if len(narrow_conditions) == 0 and 'PostalCode' in tags:
                postal_code = normalize_postal_code(tags.get('PostalCode', ''))
                if len(postal_code) == 6:
                    narrow_conditions.append(table.mail_postal_code == postal_code)

            if len(narrow_conditions) == 0 and 'PlaceName' in tags and 'ProvinceAbbreviation' in tags:
                place_simple = simplify_value(tags.get('PlaceName', ''))
                (best_matches, distance, _) = find_closest(place_simple, place_cache_simple)

                # place_closest_original = place_cache[place_cache_simple.index(best_matches[0])]

                province_abbreviation = tags.get('ProvinceAbbreviation', '').upper()

                if distance < 3 and len(province_abbreviation) == 2:
                    narrow_conditions.append(Criterion.any([
                        table.mail_mun_name == place_cache[place_cache_simple.index(match)] for match in best_matches
                    ])),
                    narrow_conditions.append(table.mail_prov_abvn == province_abbreviation)

            if len(narrow_conditions) > 0:
                # Try for a PO Box first
                if 'POBoxNumber' in tags:
                    po_box_matches = execute_log(cur, Query.from_(table).select(table.addr_guid).where(Criterion.all(narrow_conditions + [
                        table.bu_n_civic_add.ilike('PO BOX ' + tags.get('POBoxNumber', ''))
                    ])).get_sql()).fetchall()

                    if len(po_box_matches) == 1:
                        return po_box_matches[0][0]

                narrow_cache_key = Criterion.all(narrow_conditions).get_sql()
                if narrow_cache_key in narrow_cache:
                    print("CACHE HIT")
                    narrowed = narrow_cache[narrow_cache_key]
                else:
                    # Clear the cache if it is too large
                    if len(narrow_cache) > 2000:
                        print("CACHE CLEAR")
                        narrow_cache = {}

                    print("CACHE ADD")
                    narrowed = execute_log(cur, Query.from_(table).select(table.mail_street_name, table.mail_street_type, table.mail_street_dir).distinct().where(Criterion.all(narrow_conditions)).get_sql()).fetchall()
                    narrowed = narrowed + execute_log(cur, Query.from_(table).select(table.official_street_name, table.official_street_type, table.official_street_dir).distinct().where(Criterion.all(narrow_conditions)).get_sql()).fetchall()
                    narrow_cache[narrow_cache_key] = narrowed

            if narrowed is None or len(narrowed) == 0:
                log('Could not narrow')
            elif len(search_street_name) >= 3:
                log('NARROWS TO', len(narrowed), 'STREETS')

                (best_matches, best_score, best_error) = find_closest(search_street_name, yield_street_variations(narrowed))
                log('BEST STREET MATCH FOR', search_street_name, 'IS', best_matches, 'SCORE', best_score, 'ERROR', best_error)

                if best_error > 0.3:
                    log('NOT CLOSE ENOUGH')
                    return None


                # print('BEFORE', best_matches, len(best_matches))

                best_matches = set(tuple(s.upper() if isinstance(s, str) else s for s in t) for t in best_matches)

                # print('AFTER', best_matches, len(best_matches))

                street_narrow_conditions_options = []
                for best_match in best_matches:
                    street_narrow_conditions_options.append(Criterion.all([
                        table.mail_street_name.isnull() if best_match[0] is None else fn.Upper(table.mail_street_name) == best_match[0],
                        table.mail_street_type.isnull() if best_match[1] is None else fn.Upper(table.mail_street_type) == best_match[1],
                        table.mail_street_dir.isnull() if best_match[2] is None else fn.Upper(table.mail_street_dir) == best_match[2],
                    ]))
                    street_narrow_conditions_options.append(Criterion.all([
                        table.official_street_name.isnull() if best_match[0] is None else fn.Upper(table.official_street_name) == best_match[0],
                        table.official_street_type.isnull() if best_match[1] is None else fn.Upper(table.official_street_type) == best_match[1],
                        table.official_street_dir.isnull() if best_match[2] is None else fn.Upper(table.official_street_dir) == best_match[2],
                    ]))

                street_narrow_conditions = narrow_conditions + [
                    Criterion.any(street_narrow_conditions_options)
                ]

                if 'AddressNumber' in tags:
                    street_narrow_conditions.append(Criterion.any([
                        table.civic_no == tags.get('AddressNumber'),
                        fn.Concat(table.civic_no, table.civic_no_suffix).ilike(tags.get('AddressNumber')),
                    ]))

                final_query = Query.from_(table).select(table.addr_guid, table.loc_guid, table.apt_no_label, table.civic_no, table.civic_no_suffix).where(Criterion.all(street_narrow_conditions))
                log(final_query.get_sql())
                deep_results = execute_log(cur, final_query.get_sql()).fetchall()

                log("NARROWS TO", len(deep_results), "POSSIBLE ADDRESSES")

                (best_matches, best_score, best_error) = find_closest(search_numbers, yield_civic_no_variations(deep_results))
                log('BEST ADDRESS MATCH FOR', search_numbers, 'IS', best_matches[:3], 'SCORE', best_score, 'ERROR', best_error)

                # No good matches
                if len(best_matches) == 0:
                    return None
                # Only one match
                elif len(best_matches) == 1:
                    return best_matches[0][0]
                # Multiple matches, so see if they're all at the same location
                else:
                    loc_guids = set([x[1] for x in best_matches])
                    if len(loc_guids) == 1:
                        return loc_guids.pop()
                    else:
                        return None

        except canaddress.RepeatedLabelError:
            log('EDGE CASE')
