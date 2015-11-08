import random
import string
def generate_sql(line_count):

    f = lambda : (repr(random.randint(0,99999999)),
                     "".join( [random.choice(string.letters) for i in xrange(16)] ),
                     random.randint(18,30),
                     random.choice(['M','F']),
                     random.randrange(0,100)
    )

    create = "create table student (sno char(8),sname char(16) unique,sage int,sgender char (1),score float,primary key ( sno ));\n"
    sqls = [create]

    for i in xrange(line_count):
        data = f()
        sqls.append("insert into student values ('%s','%s',%d,'%s',%f);\n"%data)

    file = open("/home/coxious/insert.sql","w+")

    file.writelines(sqls)

    file.close()

if __name__ == '__main__':
    generate_sql(50000)