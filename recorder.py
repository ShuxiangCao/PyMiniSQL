import pickle
import struct
import os
import config
import buffer
import re
from b_plus_tree import BPTree
from index import FakeIndex

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

    column_to_id = {}

    if table_name in table_dict.keys():
        raise Exception('Table already exists.')

    for i in range(len(schemas)):
        if schemas[i]['type'] is 'primary_key':
            if primary_key_schema is not None:
                raise Exception('Duplicate primary key')
            primary_key_schema = schemas[i]
            del schemas[i]

    for i in range(len(schemas)):
        column_to_id[schemas[i]['name']] = i
        if schemas[i]['name'] == primary_key_schema['name']:
            primary_key_position = i


    table_dict[table_name] = {
        'record_count': 0,
        'schemas': schemas,
        'column_to_id':column_to_id,
        'primary_key_pos': primary_key_position,
        'primary_key_column': primary_key_schema['name'],
        'primary_index': None,
        'indexes': {},
        'indexes_name_to_column': {}
    }

    create_index(table_name,None,None)


def handle_value_type_pair(value,schema):

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

    if len(values) != len(schemas):
        print values
        print schemas
        raise Exception('Value number doesn\'t match')

    value_parsed = map(lambda x:handle_value_type_pair(x[0],x[1]), zip(values, schemas))

    primary_key_value = value_parsed[table_dict[table_name]['primary_key_pos']]

    data = struct.pack(format_string,*value_parsed)

    start_pos = file_object.write(data)

    table_dict[table_name]['record_count'] += 1

    table_dict[table_name]['primary_index'].insert(primary_key_value,start_pos)

    for column, index in table_dict[table_name]['indexes'].iteritems():
        id = table_dict[table_name]['column_to_id'][column]
        index.insert(value_parsed[id],start_pos)

    update_catalog_file()

def delete_index(table_name, index_name):

    if table_name not in table_dict:
        raise Exception('Table doesn\'t exsists.')

    if index_name not in table_dict[table_name]['indexes_name_to_column'].keys():
        raise Exception("Index name %s doesn\'t exists." % index_name)

    column = table_dict[table_name]['indexes_name_to_column'][index_name]

    del table_dict[table_name]['indexes_name_to_column'][index_name]
    del table_dict[table_name]['indexes'][column]

    update_catalog_file()

def create_index(table_name, index_name, column):

    if table_name not in table_dict:
        raise Exception('Table doesn\'t exsists.')

    index = BPTree(32)

    if index_name is None:
        table_dict[table_name]['primary_index'] = index
    else:

        if index_name in table_dict[table_name]['indexes_name_to_column'].keys():
            raise Exception("Index name %s already exists." % index_name)

        if column in table_dict[table_name]['indexes'].keys():
            raise Exception("Index for column %s already exists." % column)

        positions,dummy = select_record_position(table_name,None)
        record = select_record(table_name,positions,[column])

        map(lambda pair:index.insert(*pair), zip([x[0] for x in record],positions))

        table_dict[table_name]['indexes'][column] = index
        table_dict[table_name]['indexes_name_to_column'][index_name] = column

    update_catalog_file()


def select_record_position(table_name,conditions):

    table_catalog = table_dict[table_name]
    index  = table_catalog['primary_index']

    require_filter_condition = []

    pos_list =  index.values()

    if conditions is not None:
        for condition in conditions:

            id = table_catalog['column_to_id'][condition['left']]
            right_value = handle_value_type_pair(condition['right'],table_catalog['schemas'][id])

            index = None
            max = None
            min = None
            inverse = False
            range_query = True

            if condition['left'] == table_dict[table_name]['primary_key_column']:
                index = table_dict[table_name]['primary_index']

            elif condition['left'] in table_dict[table_name]['indexes']:
                index = table_dict[table_name]['indexes'][condition['left']]

            else:
                require_filter_condition.append(condition)

            if index is not None:

                if condition['op'] == '>':
                    max = right_value
                    inverse = True

                elif condition['op'] == '<':
                    min = right_value
                    inverse = True

                elif condition['op'] == '=':
                    range_query = False

                elif condition['op'] == '<>':
                    range_query = False
                    inverse = True

                elif condition['op'] == '>=':
                    min = right_value

                elif condition['op'] == '<=':
                    max = right_value

                if range_query:
                    current_pos_list = index.values(min,max)
                else:
                    current_pos_list = [index[right_value]]

                if not inverse:
                    pos_list = [x  for x in pos_list if x in current_pos_list]
                else:
                    pos_list = [x  for x in pos_list if x not in current_pos_list]

    return pos_list,require_filter_condition


def read_records(table_name,record_positions,columns):

    table_catalog = table_dict[table_name]

    file_object = get_file_object_from_table_name(table_name)

    format_string = get_format_string_from_schema(table_catalog['schemas'])

    record_length = get_each_record_size(table_catalog['schemas'])

    data_string_list = [ file_object.read(pos,record_length) for pos in record_positions]

    records_list = map(lambda data_str:[ (x.strip('\x00')
                        if type(x) is str else x) for x in struct.unpack(format_string,data_str)],
                       data_string_list)

    return records_list


def select_record(table_name,columns,conditions):

    table_catalog = table_dict[table_name]

    positions, conditions = select_record_position(table_name, conditions)

    if '*' in columns:
        columns = table_catalog['column_to_id'].keys()

    least_column = columns

    for condition in conditions:
        if condition['left'] not in least_column:
            least_column.append(condition['left'])

    records = read_records(table_name, positions, least_column)

    query_column_ids = [x for x in range(len(least_column)) if least_column[x] in columns]

    for condition in conditions:

        id = table_catalog['column_to_id'][condition['left']]
        right_value = handle_value_type_pair(condition['right'], table_catalog['schemas'][id])

        if condition['op'] == '>':
            records = [x for x in records if x[id] > right_value]
        elif condition['op'] == '<':
            records = [x for x in records if x[id] < right_value]
        elif condition['op'] == '=':
            records = [x for x in records if x[id] == right_value]
        elif condition['op'] == '<>':
            records = [x for x in records if x[id] != right_value]
        elif condition['op'] == '>=':
            records = [x for x in records if x[id] >= right_value]
        elif condition['op'] == '<=':
            records = [x for x in records if x[id] <= right_value]

    records = [[record[x] for x in query_column_ids] for record in records]

    return records

def delete_records(table_name,conditions):

    table_catalog = table_dict[table_name]

    columns = table_dict[table_name]['indexes'].keys()

    if table_dict[table_name]['primary_key_column'] not in columns:
        columns.append(table_dict[table_name]['primary_key_column'])

    records_to_delete = select_record(table_name,columns,conditions)


    pass

def debug(table_name):
    print table_dict[table_name]['indexes']

init_table_file()
