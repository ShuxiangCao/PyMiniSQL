import re
import recorder
import sys

from prettytable import PrettyTable

sys.setrecursionlimit(1000000)

def result_to_table(result):
    if len(result) == 0:
        return "Empty set."

    columns = result[0].keys()
    x = PrettyTable(columns)
    x.padding_width = 1 # One space between column edges and contents (default)

    map(lambda row:x.add_row(row.values()),result)

    return x

class Tokenizer(object):

    def __init__(self, sql_query):
        self.query = sql_query

        self.tokenizer = {
            "drop_table": self.drop_table,
            "drop_index": self.drop_index,
            "create_table": self.create_table,
            "create_index": self.create_index,
            "select": self.select,
            "insert": self.insert,
            "delete": self.delete,
            "nop": self.nop
        }

    def tokenize(self):
        for op, func in self.tokenizer.iteritems():
            result = func()
            if result:
                return result

    def nop(self):
        matches = re.match('^\s*;', self.query)
        return{
            'op': 'nop'
        } if matches else None

    def drop_index(self):
        matches = re.match('drop\s+index\s+(?P<index_name>\S+)\s*;', self.query)
        return{
            'op': 'drop_index',
            'index_name': matches.group('index_name'),
        } if matches else None

    def create_index(self):
        matches = re.match(
            'create\s+index\s+(?P<index_name>\S+)\s+on\s+(?P<table_name>\S+)\s*\(\s*(?P<key>\S+)\s*\)\s*;',
            self.query)
        return{
            'op': 'create_index',
            'table_name': matches.group('table_name'),
            'index_name': matches.group('index_name'),
            'key': matches.group('key'),
        } if matches else None

    def drop_table(self):
        matches = re.match('drop\s+table\s+(?P<table_name>\S+)\s*;', self.query)
        return{
            'op': 'drop_table',
            'table_name': matches.group('table_name'),
        } if matches else None

    def create_table(self):

        def match_schema(schema):
            primary_key_match = re.match(r'\s*primary\s+key\s*\(\s*(?P<key>\S+)\s*\)\s*', schema)

            if primary_key_match:
                return {
                    'type': 'primary_key',
                    'name': primary_key_match.group('key')
                }

            schema_match = re.match(r'(?P<name>\S+)\s*(?P<type>\S+(\s*\(\d+\))?)\s*(?P<extra>.*)', schema)

            char_match = re.match(r'char\s*\((?P<length>\d+)\)\s*', schema_match.group('type'))

            unique_match = re.match('\s*unique\s*', schema_match.group('extra'))

            schema_dict = {
                'name': schema_match.group('name'),
                'unique': unique_match is not None
            }

            if char_match:
                schema_dict['type'] = 'char'
                schema_dict['length'] = int(char_match.group('length'))
                if schema_dict['length'] is None or schema_dict['length'] < 1:
                    raise Exception('Char length is not correctly specified.')
            else:
                schema_dict['type'] = schema_match.group('type')

            return schema_dict

        exp_base = re.compile('\s*create\s+table\s+(?P<table_name>\S+)\s*\(\s*(?P<schema>.+)\s*\)\s*;')
        matches = exp_base.match(self.query)

        if not matches:
            return None

        schemas = re.compile(',\s*').split(matches.group('schema'))

        return{
            'op': 'create_table',
            'table_name': matches.group('table_name'),
            'schemas': map(match_schema, schemas)
        }

    def match_conditions(self, extra):
        if not extra:
            return None

        conditions_match = re.match(r'where\s+(?P<conditions>.+)\s*', extra)
        conditions = re.split(r'\s*and\s*', conditions_match.group('conditions'))

        def match_each_conditions(condition):
            matches = re.match(r'(?P<left>\S+)\s*(?P<op>(<=|>=|=|<>|>|<))\s*(?P<right>.+)\s*', condition)
            return {
                'left': matches.group('left'),
                'right': matches.group('right'),
                'op': matches.group('op')
            }
        return map(match_each_conditions, conditions)

    def select(self):

        def match_colums(colunms):
            return re.split(r'\s*,\s*', colunms)

        exp_base = re.compile('select\s+(?P<colunms>[\w\d\*,]+)\s+from\s+(?P<table_name>\S+)\s*(?P<extra>.+)?\s*;')
        matches = exp_base.match(self.query)

        if not matches:
            return None

        return {
            'op': 'select',
            'table_name': matches.group('table_name'),
            'colunms': match_colums(matches.group('colunms')),
            'conditions': self.match_conditions(matches.group('extra'))
        }

    def insert(self):
        exp_base = re.compile('insert\s+into\s+(?P<table_name>\S+)\s+values\s+\((?P<values>.+)\)\s*;')
        matches = exp_base.match(self.query)

        if not matches:
            return None

        return {
            "op": 'insert',
            "table_name": matches.group("table_name"),
            "values": re.split(r"\s*,\s*", matches.group("values"))
        }

    def delete(self):

        exp_base = re.compile('delete\s+from\s+(?P<table_name>\S+)\s*(?P<extra>.+)?\s*;')
        matches = exp_base.match(self.query)

        if not matches:
            return None

        return {
            'op': 'delete',
            'table_name': matches.group('table_name'),
            'conditions': self.match_conditions(matches.group('extra'))
        }

def flush():
    recorder.update_catalog_file()

def do_query(query):

    sql_tokenizer = Tokenizer(query)
    try:
        op_dict = sql_tokenizer.tokenize()
    except:
        raise Exception('Syntax error')
    if op_dict is None:
        raise Exception('Syntax error')

    ret = None

    if op_dict['op'] == 'create_table':
        ret = recorder.create_table_file(op_dict['table_name'], op_dict['schemas'])
    elif op_dict['op'] == 'insert':
        ret = recorder.insert_record(op_dict['table_name'], op_dict['values'])
    elif op_dict['op'] == 'select':
        records = recorder.select_record(op_dict['table_name'],op_dict['colunms'],op_dict['conditions'])
        ret = result_to_table(records)
    elif op_dict['op'] == 'create_index':
        ret = recorder.create_index(op_dict['table_name'],op_dict['index_name'],op_dict['key'])
    elif op_dict['op'] == 'drop_index':
        ret = recorder.delete_index(op_dict['index_name'])
    elif op_dict['op'] == 'delete':
        ret = recorder.delete_records(op_dict['table_name'],op_dict['conditions'])
    elif op_dict['op'] == 'drop_table':
        ret = recorder.delete_table_file(op_dict['table_name'])
    elif op_dict['op'] == 'nop':
        return None
    else:
        raise Exception("%s doesn\'t support" % query)

    return ret

if __name__ == '__main__':

    test_banch_1 = [
        "create table student (sno char(8),sname char(16) unique,sage int,sgender char (1),score float,primary key ( sno ));",
        "delete from student;",
        "insert into student values ('12345678',	'wy1',22,'M',95);",
        "insert into student values ('12345679',	'wy2',19,'F',100);",
        "create index stunameidx on student ( sname );",
        "insert into student values ('12345682',	'wy5',14,'M',60);",
        "insert into student values ('12345684',	'wy6',25,'F',63);",
        "select * from student;",
        "select * from student where score >= 98;",
        "select * from student where sage > 20 and sgender = 'F';",#,
        "delete from student where sno = '12345678';",
        "delete from student where sname = 'wy2';",
        "select * from student;",
        "insert into student values ('12345681',	'wy4',23,'F',96);",
        "insert into student values ('12345670',	'wy3',25,'M',0);",
        "select * from student where score < 10;",
        "select * from student where sgender <> 'F';",
        "drop index stunameidx;",
        "drop table student;"
    ]

    test_banch_2 = [
        "create table orders (	orderkey int, custkey int unique,orderstatus char(1),totalprice	float,clerk char(15),comments char(79) unique,primary key(orderkey));",
        "select * from orders where orderkey=182596;",
        "select * from orders where totalprice > 500000;",
        "insert into orders values (541959,408677,'F',241827.84,'Clerk#000002574','test: check unique');",
        "create index custkeyidx on orders (custkey);",
        "insert into orders values (541959,408677,'F',241827.84,'Clerk#000002574','test: check unique');",
        "select * from orders;",
        "delete from orders where custkey > 430000;",
        "select * from orders;",
        "drop index custkeyidx;",
        "select * from orders;",
        "create index commentsidx on orders (comments);",
        "delete from orders where custkey=408677;",
        "select * from orders;",
        "insert into orders values (541959,408677,'F',241827.84,'Clerk#000002574','test: check unique');",
        "select * from orders;",
        "select * from orders where orderstatus='O' and comments='test: check unique';",
        "select * from orders where totalprice > 183500 and totalprice < 190000 and comments='en asymptotes are carefully qu';",
        "select * from orders where totalprice > 10 and totalprice < 1;",
        "delete from orders;",
        "select * from orders;",
        "drop table orders;"
    ]

    sql_1 = 'create table student (sno char(8),sname char(16) unique,sage int,sgender char (1),primary key ( sno ));'
    sql_2 = 'drop table student;'
    sql_3 = 'create index stunameidx on student ( sname );'

    sql_4 = "select * from student;"
    sql_5 = "select name,a,b from student where sno = '88888888';"
    sql_6 = "select * from student where sage > 20 and sgender = 'F';"

    sql_7 = "insert into student values ('12345678','wy',22,'M');"
    sql_8 = "delete from student where sno = '88888888';"

    sql_9 = "select * from student where sgender = 'M';"

    sql_10 = "select sname,sno from student where sno = '12345678';"

    sql_11 = "select * from student where sage > 20 and sgender = 'F';"

    #do_query(sql_4)
    #do_query(sql_7)
    #do_query(sql_9)

    #do_query(sql_11)
    def run(debug = True):
        for sql in test_banch_2:
            if not debug:
                try:
                    print do_query(sql)
                except Exception,e:
                    print e
            else:
                print sql
                ret = do_query(sql)
                if ret:
                    print ret

    run(False)
    #map(do_query,test_banch_2)
    #do_query(sql_9)
