import pickle
import struct
import os
import config
import buffer
import re
from b_plus_tree import BPTree
from index import FakeIndex

table_dict = {}
index_dict = {}


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
    global index_dict

    if os.path.isfile(config.table_file):
        table_file = open(config.table_file, 'rb')
        table_dict = pickle.load(table_file)
        table_file.close()

    if os.path.isfile(config.index_file):
        index_file = open(config.index_file, 'rb')
        index_dict = pickle.load(index_file)
        index_file.close()


def update_catalog_file():

    table_file = open(config.table_file, 'wb')
    pickle.dump(table_dict, table_file)
    table_file.close()

    index_file = open(config.index_file, 'wb')
    pickle.dump(index_dict, index_file)
    index_file.close()


def create_table_file(table_name, schemas):

    primary_key_schema = None
    primary_key_position = 0

    column_to_id = {}
    column_is_unique = {}

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

        column_is_unique[schemas[i]['name']] = schemas[i]['unique']

    table_dict[table_name] = {
        'record_count': 0,
        'schemas': schemas,
        'column_to_id':column_to_id,
        'primary_key_pos': primary_key_position,
        'primary_key_column': primary_key_schema['name'],
        'primary_index': None,
        'indexes': {},
        'unique':column_is_unique
    }

    create_index(table_name,None,None)

    for i in range(len(schemas)):
        if schemas[i]['unique']:
            create_index(table_name,"#_unique_%s" % schemas[i]['name'],schemas[i]['name'])

    update_catalog_file()


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

    for index_name,index_specifier in index_dict.items():
        index_table_name = index_specifier[0]
        if index_table_name == table_name:
            del index_dict[index_name]

    del table_dict[table_name]

    file_object = get_file_object_from_table_name(table_name)
    file_object.delete()

    update_catalog_file()

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

    for column,unique in table_dict[table_name]['unique'].iteritems():
        if unique:
            index = table_dict[table_name]['indexes'][column]
            id = table_dict[table_name]['column_to_id'][column]
            key = value_parsed[id]

            if key in index.keys():
                raise Exception("Duplicate unique key.")

    primary_key_value = value_parsed[table_dict[table_name]['primary_key_pos']]

    data = struct.pack(format_string,*value_parsed)

    start_pos = file_object.write(data)

    table_dict[table_name]['record_count'] += 1

    table_dict[table_name]['primary_index'].insert(primary_key_value,start_pos)

    for column, index in table_dict[table_name]['indexes'].iteritems():
        id = table_dict[table_name]['column_to_id'][column]
        index.insert(value_parsed[id],start_pos)

    update_catalog_file()

def delete_index(index_name):

    if index_name not in index_dict.keys():
        raise Exception("Index name %s doesn\'t exists." % index_name)

    table_name = index_dict[index_name][0]
    column = index_dict[index_name][1]

    del index_dict[index_name]

    if not table_dict[table_name]['unique'][column]:
        del table_dict[table_name]['indexes'][column]

    update_catalog_file()

def create_index(table_name, index_name, column):

    if table_name not in table_dict:
        raise Exception('Table doesn\'t exsists.')

    index = BPTree(32)

    if index_name is None:
        table_dict[table_name]['primary_index'] = index
    else:

        if index_name in index_dict.keys():
            raise Exception("Index name %s already exists." % index_name)

        if column in table_dict[table_name]['indexes'].keys():
            if table_dict[table_name]['unique'][column]:

                index_dict[index_name] = [table_name,column]
                update_catalog_file()

                return

            raise Exception("Index for column %s already exists." % column)

        positions,dummy = select_record_position(table_name,None)
        record = read_records(table_name,positions,[column])

        map(lambda pair:index.insert(*pair), zip([x[column] for x in record],positions))

        table_dict[table_name]['indexes'][column] = index

        index_dict[index_name] = [table_name,column]

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

    pos_list = [ y for x in pos_list for y in x]

    return pos_list,require_filter_condition


def read_records(table_name,record_positions,columns,with_position = False):

    table_catalog = table_dict[table_name]

    file_object = get_file_object_from_table_name(table_name)

    format_string = get_format_string_from_schema(table_catalog['schemas'])

    record_length = get_each_record_size(table_catalog['schemas'])

    data_string_list = [ file_object.read(pos,record_length) for pos in record_positions]


    raw_records_list = map(lambda data_str:[ (x.strip('\x00')
                        if type(x) is str else x) for x in struct.unpack(format_string,data_str)],
                       data_string_list)

    record_list = []

    for i in range(len(raw_records_list)):
        record = raw_records_list[i]

        current_record_dict = {}

        for column,id in table_catalog['column_to_id'].iteritems():
            current_record_dict[column] = record[id]

        if with_position:
            current_record_dict['#_pos'] = record_positions[i]

        record_list.append(current_record_dict)

    return record_list


def select_record(table_name,columns,conditions,with_position = False):

    table_catalog = table_dict[table_name]

    positions, conditions = select_record_position(table_name, conditions)

    if '*' in columns:
        columns = [schema['name'] for schema in table_catalog['schemas']]

    least_column = columns

    for condition in conditions:
        if condition['left'] not in least_column:
            least_column.append(condition['left'])

    records = read_records(table_name, positions, least_column, with_position)

    column_to_id_current_columns = {}
    for i in range(len(least_column)):
        column_to_id_current_columns[least_column[i]] = i

    for condition in conditions:

        id = table_catalog['column_to_id'][condition['left']]
        right_value = handle_value_type_pair(condition['right'], table_catalog['schemas'][id])

        if condition['op'] == '>':
            records = [x for x in records if x[condition['left']] > right_value]
        elif condition['op'] == '<':
            records = [x for x in records if x[condition['left']] < right_value]
        elif condition['op'] == '=':
            records = [x for x in records if x[condition['left']] == right_value]
        elif condition['op'] == '<>':
            records = [x for x in records if x[condition['left']] != right_value]
        elif condition['op'] == '>=':
            records = [x for x in records if x[condition['left']] >= right_value]
        elif condition['op'] == '<=':
            records = [x for x in records if x[condition['left']] <= right_value]

    if with_position:
        columns.append('#_pos')

    def filter_record(record):
        record_dict = {}
        for x in columns:
            record_dict[x] = record[x]
        return record_dict

    return  [filter_record(record) for record in records ]

def delete_records(table_name,conditions):

    columns = table_dict[table_name]['indexes'].keys()

    if table_dict[table_name]['primary_key_column'] not in columns:
        columns.append(table_dict[table_name]['primary_key_column'])

    records_to_delete = select_record(table_name,columns,conditions,True)

    for record in records_to_delete:
        for record_column,data in record.iteritems():

            if table_dict[table_name]['primary_key_column'] == record_column:
                index = table_dict[table_name]['primary_index']
            else:
                if record_column in table_dict[table_name]['indexes'].keys():
                    index = table_dict[table_name]['indexes'][record_column]
                else:
                    index = None

            if index is not None:
                index.delete(data,record['#_pos'])

    update_catalog_file()

def debug(table_name):
    print table_dict[table_name]['indexes']

init_table_file()
