from b_plus_tree import *
import os

opened_file_dict = {}

def is_file_opened(path):
    return path in opened_file_dict.keys()


def get_file_object(path):
    if is_file_opened(path):
        return opened_file_dict[path]
    else:
        return CachedFile(path)


class CachedFile(object):

    def __init__(self,file):
        self.raw_file = open(file,'a+b')
        self.deleted = False
        self.path = file

        if is_file_opened(file):
            raise Exception('Duplicate file opening.')

        opened_file_dict[self.path] = self

    def __del__(self):
        if not self.deleted:
            self.raw_file.close()

    def delete(self):
        os.remove(self.path)
        self.deleted = True

    def read(self,offset,size):

        if self.deleted:
            raise Exception("File already deleted.")

        self.raw_file.seek(offset)
        return self.raw_file.read(size)

    def write(self,data,offset=None):

        if self.deleted:
           raise Exception("File already deleted.")

        if offset is None:
            self.raw_file.seek(0,2)
        else:
            self.raw_file.seek(offset)


        start_pos = self.raw_file.tell()
        self.raw_file.write(data)
        self.raw_file.flush()

        return start_pos
