class FakeIndex():
    mapping = {}
    def insert(self,key,val):
        self.mapping[key] = val
    def delete(self,key):
        del self.mapping[key]
    def query(self,key):
        return self.mapping[key]
    def iterkeys(self):
        return self.mapping.iterkeys()
    def itervalues(self):
        return self.mapping.itervalues()
