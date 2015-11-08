import api
import re
import sys
import time

def exec_file(path):
    print 'File %s'%path
    file = open(path,'r')
    lines = file.readlines()

    string = "".join([x.strip("\n") for x in lines]).strip("\n")
    lines = map(lambda x:x+';',string.split(';'))

    for line in lines:
        try:
            print "[+] %s" % line
            ret = api.do_query(line)
            if ret:
                print ret
        except Exception,e:
            print "[-]Error : %s" % e


def main():

    while(True):

        print "----"
        str_input = sys.stdin.readline().strip("\n")

        if str_input == 'quit;':
            break

        match = re.match(r'execfile\s+(?P<path>.+)\s*;',str_input)

        if match:
            start = time.time()
            exec_file(match.group('path'))
            end = time.time()
            print "Time escaped %f " % (end-start)

        else:
            start = time.time()
            try:
                print api.do_query(str_input)
            except Exception,e:
                print "[-]Error : %s" % e
            end = time.time()
            print "Time escaped %f " % (end-start)

if __name__ == '__main__':
    main()
