import re

class Tokenizer(object):

    def __init__(self,query):
        self.query = query

        self.tokenizer = {
            "drop_table":self.drop_table,
            "drop_index":self.drop_index,
            "create_table":self.create_table,
            "create_index":self.create_index,
            "select":self.select,
            "insert":self.insert,
            "delete":self.delete
        }

    def tokenize(self):
        for op,func in self.tokenizer.iteritems():
            result = func()
            if result:
                return result

    def drop_index(self):
        matches = re.match('create\s+index\s+(?P<index_name>\S+)\s*;',self.query)
        return{
            'op':'drop_index',
            'index_name':matches.group('index_name'),
        } if matches else None

    def create_index(self):
        matches = re.match('create\s+index\s+(?P<index_name>\S+)\s+on\s+(?P<table_name>\S+)\s*\(\s*(?P<key>\S+)\s*\)\s*;',self.query)
        return{
            'op':'create_index',
            'table_name':matches.group('table_name'),
            'index_name':matches.group('index_name'),
            'key':matches.group('key'),
        } if matches else None

    def drop_table(self):
        matches = re.match('drop\s+table\s+(?P<table_name>\S+)\s*;',self.query)
        return{
            'op':'drop_table',
            'table_name':matches.group('table_name'),
        } if matches else None

    def create_table(self):
        def match_schema(schema):
            primary_key_match = re.match(r'\s*primary\s+key\s*\((?P<key>\S+)\)\s*;',schema)

            if primary_key_match:
                return {
                            'type':'primary_key',
                            'name':primary_key_match.group('key')
                        }

            schema_match = re.match(r'(?P<name>\S+)\s*(?P<type>\S+(\s*\(\d+\))?)\s*(?P<extra>.*)',schema)

            char_match = re.match(r'char\s*\((?P<length>\d+)\)\s*',schema_match.group('type'))

            unique_match = re.match('\s*unique\s*',schema_match.group('extra'))

            schema_dict = {
                'name':schema_match.group('name'),
                'unique':type(unique_match) != type(None)
            }

            if char_match:
                schema_dict['type'] = 'char'
                schema_dict['length'] = char_match.group('length')
            else:
                schema_dict['type'] = schema_match.group('type')

            return schema_dict

        exp_base = re.compile('create\s+table\s+(?P<table_name>\S+)\s\((?P<schema>.+)\)\s*')
        matches = exp_base.match(self.query)

        if not matches :
            return None

        schemas = re.compile(',\s*').split(matches.group('schema'))

        return{
            'op':'create_table',
            'table_name':matches.group('table_name'),
            'schemas':map(match_schema,schemas)
        }

    def match_conditions(self,extra):
       if not extra:
           return None

       conditions_match = re.match(r'where\s+(?P<conditions>.+)\s*',extra)
       conditions= re.split(r'\s*and\s*',conditions_match.group('conditions'))

       def match_each_conditions(condition):
           matches = re.match(r'(?P<left>\S+)\s*(?P<op>(\<|\>|\=|\<\>|\>\=|\<\=))\s*(?P<right>\S+)\s*',condition)
           return {
               'left':matches.group('left'),
               'right':matches.group('right'),
               'op':matches.group('op')
           }
       return map(match_each_conditions,conditions)

    def select(self):

        def match_colums(colunms):
            return re.split(r'\s*\,\s*',colunms)

        exp_base = re.compile('select\s+(?P<colunms>[\w\d\*\,]+)\s+from\s+(?P<table_name>\S+)\s*(?P<extra>.+)?\s*;')
        matches = exp_base.match(self.query)

        if not matches:
            return None

        return {
            'op':'select',
            'table_name':matches.group('table_name'),
            'colunms':match_colums(matches.group('colunms')),
            'conditions':self.match_conditions(matches.group('extra'))
        }

    def insert(self):
        exp_base = re.compile('insert\s+into\s+(?P<table_name>\S+)\s+values\s+\((?P<values>.+)\)\s*;')
        matches = exp_base.match(self.query)

        if not matches:
            return None

        return {
            "op":'insert',
            "table_name":matches.group("table_name"),
            "values":re.split(r"\s*\,\s*",matches.group("values"))
        }

    def delete(self):

        exp_base = re.compile('delete\s+from\s+(?P<table_name>\S+)\s*(?P<extra>.+)?\s*;')
        matches = exp_base.match(self.query)

        if not matches:
            return None

        return {
            'op':'delete',
            'table_name':matches.group('table_name'),
            'conditions':self.match_conditions(matches.group('extra'))
        }

if __name__ == '__main__':
    sql_1 = 'create table student (sno char(8),sname char(16) unique,sage int,sgender char (1),primary key ( sno ));'
    sql_2 = 'drop table student;'
    sql_3 = 'create index stunameidx on student ( sname );'

    sql_4 = "select * from student;"
    sql_5 = "select name,a,b from student where sno = '88888888';"
    sql_6 = "select * from student where sage > 20 and sgender = 'F';"

    sql_7 = "insert into student values ('12345678','wy',22,'M');"
    sql_8 = "delete from student where sno = '88888888';"

    tokenizer = Tokenizer(sql_7)
    print tokenizer.tokenize()
