import pickle
import struct
import os
import config
import buffer
import re

table_dict = {}


def get_file_path_from_table_name(table_name):
    return config.table_path + table_name


def get_file_object_from_table_name(table_name):
    return buffer.get_file_object(get_file_path_from_table_name(table_name))


def get_each_record_size(schemas):
    format_str = get_format_string_from_schema(schemas)
    return struct.calcsize(format_str)


def get_format_string_from_schema(schemas):
    format_str = ""
    for schema in schemas:
        if schema['type'] == 'int':
            format_str += 'i'
        elif schema['type'] == 'float':
            format_str += 'f'
        elif schema['type'] == 'char' and schema['length'] is not None:
            format_str += '%ds' % schema['length']
        elif schema['type'] == 'primary_key':
            pass
        else:
            raise Exception("Schema syntax error.")
    return format_str


def init_table_file():
    global table_dict

    if os.path.isfile(config.table_file):
        catalog_file = open(config.table_file, 'rb')
        table_dict = pickle.load(catalog_file)
        catalog_file.close()


def update_catalog_file():
    catalog_file = open(config.table_file, 'wb')
    pickle.dump(table_dict, catalog_file)
    catalog_file.close()


def create_table_file(table_name, schemas):

    primary_key_schema = None
    primary_key_position = 0

    if table_name in table_dict.keys():
        raise Exception('Table already exists.')

    for i in range(len(schemas)):
        if schemas[i]['type'] is 'primary_key':
            if primary_key_schema is not None:
                raise Exception('Duplicate primary key')
            primary_key_schema = schemas[i]
            del schemas[i]

    for i in range(len(schemas)):
        if schemas[i]['name'] == primary_key_schema['name']:
            primary_key_position = i
            break

    table_dict[table_name] = {
        'record_count': 0,
        'schemas': schemas,
        'primary_key':primary_key_position
    }


def delete_table_file(table_name):
    if table_name not in table_dict.keys():
        raise Exception("Table doesn't exists.")
    del table_dict[table_name]

    file_object = get_file_object_from_table_name(table_name)
    file_object.delete()


def insert_record(table_name, values):

    table_catalog = table_dict[table_name]

    if table_catalog is None:
        raise Exception('Table doesn\'t exists.')

    file_object = get_file_object_from_table_name(table_name)

    format_string = get_format_string_from_schema(table_catalog['schemas'])

    schemas = table_catalog['schemas']

    def handle_value_type_pair(value_type_tuple):
        value = value_type_tuple[0]
        schema = value_type_tuple[1]

        if schema['type'] == 'char':
            matches = re.match(r'\'(?P<string>.+)\'', value)
            if matches is None:
                raise Exception("Char value syntax error.")
            return matches.group('string')

        elif schema['type'] == 'int':
            return int(value)

        elif schema['type'] == 'float':
            return float(value)
        else:
            raise Exception('Unexpected type: %s' % schema['type'])

    if len(values) != len(schemas):
        print values
        print schemas
        raise Exception('Value number doesn\'t match')

    data = struct.pack(format_string,
        *map(handle_value_type_pair, zip(values, schemas)))

    start_pos = file_object.write(data)

    table_dict[table_name]['record_count'] += 1

    update_catalog_file()

    print start_pos

def query_record(table_name, index):

    table_catalog = table_dict[table_name]

    pass

init_table_file()
