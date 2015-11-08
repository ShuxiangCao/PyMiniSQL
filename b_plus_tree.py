#
# This file is a copy of https://github.com/yflau/pybtree/blob/master/pybtree.pyb
# I modified it so that it supports one key with more than one value.
#

from itertools import izip_longest
import bisect
import Queue

class BPNode(object):

    def __init__(self):
        self.keys = list()
        self.values = list()
        self.children = list()
        self.next = None

    def is_leaf(self):
        return not bool(self.children)

    def min(self):
        node = self
        while node.children:
            node = node.children[0]
        return node

    def max(self):
        node = self
        while node.children:
            node = node.children[-1]
        return node

    def __str__(self):
        return '|%s|' % ' '.join(['{%s:%s}' % e for e in izip_longest(self.keys, self.values)])

    __repr__ = __str__


class BPTree(object):

    def __init__(self, degree = 3):
        self.degree = degree
        self.root = BPNode()

        self._minkeys = self.degree - 1
        self._minchildren = self.degree
        self._maxkeys = 2 * self.degree - 1
        self._maxchildren = 2 * self.degree
        #self.disk_write(self.root)

    def search(self, node, key):
        i = bisect.bisect_left(node.keys, key)
        if i < len(node.keys) and key == node.keys[i]:
            if node.is_leaf():
                return (node, i)
            else:
                return self.search(node.children[i+1], key)
        if node.is_leaf():
            return (None, None)
        else:
            # self.disk_read(node.children[i])
            return self.search(node.children[i], key)

    def ceiling(self, node, key):
        i = bisect.bisect(node.keys, key)
        if i < len(node.keys) and key == node.keys[i]:
            if node.is_leaf():
                return key
            else:
                return self.ceiling(node.children[i+1], key)
        if node.is_leaf():
            if i == len(node.keys):
                kp = node.keys[-1]
                if node.keys[-1] < key:
                    if len(node.next.keys) > 0:
                        return node.next.keys[0]
                else:
                    return kp
            return node.keys[i]
        else:
            return self.ceiling(node.children[i], key)

    def split_child(self, x, i, y):
        z = BPNode()
        z.keys = y.keys[self.degree:]
        z.values = y.values[self.degree:]
        if not y.is_leaf():
            z.children = y.children[self.degree:]
            y.next = None
        else:
            z.keys.insert(0, y.keys[self.degree-1])
            z.values.insert(0, y.values[self.degree-1])
            z.next = y.next
            y.next = z
        x.children.insert(i+1, z)
        x.keys.insert(i, y.keys[self.degree-1])
        #x.values.insert(i, y.values[self.degree-1])
        y.keys = y.keys[:self.degree-1]
        y.values = y.values[:self.degree-1]
        y.children = y.children[:self.degree]
        #self.disk_write(y)
        #self.disk_write(z)
        #self.disk_write(x)

    def insert(self, key, value):

        if key in self.keys():
            self[key].append(value)
            return

        if len(self.root.keys) == self._maxkeys:
            oldroot = self.root
            self.root = BPNode()
            self.root.children.append(oldroot)
            self.split_child(self.root, 0, oldroot)
            self.insert_nonfull(self.root, key, [value])
        else:
            self.insert_nonfull(self.root, key, [value])

    def insert_nonfull(self, x, key, value):
        # performance bottleneck fixed by bisect
        #while i > 0 and key < x.keys[i-1]:
        #    i -= 1
        i = bisect.bisect_left(x.keys, key)
        if x.is_leaf():
            x.keys.insert(i, key)
            x.values.insert(i, value)
            #self.disk_write(x)
        else:
            #self.disk_read(x.children[i])
            if len(x.children[i].keys) == self._maxkeys:
                self.split_child(x, i, x.children[i])
                if key > x.keys[i]:
                    i += 1
            self.insert_nonfull(x.children[i], key, value)

    def delete(self, key ,value):
        if self[key] is None:
            return

        self[key].remove(value)

        if len(self[key]) == 0:
            self._delete(self.root, key)

    def _delete(self, node, key):
        """fixed!!!"""
        if key in node.keys:
            if node.is_leaf():
                index = node.keys.index(key)
                node.keys.pop(index)
                node.values.pop(index)
            else:
                ki = node.keys.index(key)
                if len(node.children[ki].keys) >= self.degree:
                    nmax = node.children[ki].max()
                    nmin = node.children[ki+1].min()
                    kp = nmax.keys[-1]
                    self._delete(node.children[ki], kp)
                    node.keys[ki] = kp
                    nmin.keys[0] = kp
                    nmin.values[0] = nmax.values[-1]
                elif len(node.children[ki+1].keys) >= self.degree:
                    nmin = node.children[ki+1].min()
                    nmin.keys.pop(0)
                    nmin.values.pop(0)
                    kp = nmin.keys[0]
                    node.keys[ki] = nmin.keys[0]
                else:
                    rnode = node.children.pop(ki+1)
                    if node.children[ki].is_leaf():
                        node.keys.pop(ki)
                        node.children[ki].values.extend(rnode.values)
                        node.children[ki].next = rnode.next
                    else:
                        node.children[ki].keys.append(node.keys.pop(ki))
                        node.children[ki].keys.extend(rnode.keys)
                        node.children[ki].children.extend(rnode.children)
                    if node == self.root and not node.keys:
                        self.root = node.children[ki]
                    self._delete(node.children[ki], key)
        else:
            ci = bisect.bisect_left(node.keys, key)
            if len(node.children[ci].keys) == self._minkeys:
                if ci >= 1 and len(node.children[ci-1].keys) > self._minkeys:
                    if node.children[ci].is_leaf():
                        kp = node.children[ci-1].keys.pop(-1)
                        vp = node.children[ci-1].values.pop(-1)
                        node.keys[ci-1] = kp
                        node.children[ci].keys.insert(0, kp)
                        node.children[ci].values.insert(0, vp)
                    else:
                        node.children[ci].keys.insert(0, node.keys[ci-1])
                        node.keys[ci-1] = node.children[ci-1].keys.pop(-1)
                        node.children[ci].children = node.children[ci-1].children[-1:] + node.children[ci].children
                        node.children[ci-1].children = node.children[ci-1].children[:-1]
                    self._delete(node.children[ci], key)
                elif ci < len(node.keys) and len(node.children[ci+1].keys) > self._minkeys:
                    if node.children[ci].is_leaf():
                        kp = node.children[ci+1].keys.pop(0)
                        vp = node.children[ci+1].values.pop(0)
                        node.children[ci].keys.append(kp)
                        node.children[ci].values.append(vp)
                        node.keys[ci] = node.children[ci+1].keys[0]
                    else:
                        node.children[ci].keys.append(node.keys[ci])
                        node.keys[ci] = node.children[ci+1].keys.pop(0)
                        node.children[ci].children.extend(node.children[ci+1].children[:1])
                        node.children[ci+1].children = node.children[ci+1].children[1:]
                    self._delete(node.children[ci], key)
                else:
                    if ci >= 1:
                        rnode = node.children.pop(ci)
                        if node.children[ci-1].is_leaf():
                            node.keys.pop(ci-1)
                            node.children[ci-1].keys.extend(rnode.keys)
                            node.children[ci-1].values.extend(rnode.values)
                            node.children[ci-1].next = rnode.next
                        else:
                            node.children[ci-1].keys.append(node.keys.pop(ci-1))
                            node.children[ci-1].keys.extend(rnode.keys)
                            node.children[ci-1].children.extend(rnode.children)
                        if node == self.root and not node.keys:
                            self.root = node.children[ci-1]
                        self._delete(node.children[ci-1], key)
                    else:
                        rnode = node.children.pop(ci+1)
                        if node.children[ci].is_leaf():
                            node.keys.pop(ci)
                            node.children[ci].keys.extend(rnode.keys)
                            node.children[ci].values.extend(rnode.values)
                            node.children[ci].next = rnode.next
                        else:
                            node.children[ci].keys.append(node.keys.pop(ci))
                            node.children[ci].keys.extend(rnode.keys)
                            node.children[ci].children.extend(rnode.children)
                        if node == self.root and not node.keys:
                            self.root = node.children[ci]
                        self._delete(node.children[ci], key)
            else:
                self._delete(node.children[ci], key)

    def keys(self, kmin = None, kmax = None):
        keys = []

        if self.is_empty():
            return []

        if kmin is None:
            kmin = self.min()
        if kmax is None:
            kmax = self.max()

        return self._keys(self.root, kmin, kmax, keys)

    def _keys(self, node, kmin, kmax, keys):
        """return [k for k in allkeys if kmin <= k <= kmax]"""
        imin = bisect.bisect_left(node.keys, kmin)
        imax = bisect.bisect(node.keys, kmax)

        if node.children:
            for e in node.children[imin:imax+1]:
                self._keys(e, kmin, kmax, keys)
        if node.is_leaf():
            keys.extend(node.keys[imin:imax])

        return keys

    def iterkeys(self, kmin = None, kmax = None):
        if kmin is None:
            kmin = self.min()
        if kmax is None:
            kmax = self.max()

        return self._iterkeys(self.root, kmin, kmax)

    def _iterkeys(self, node, kmin, kmax):
        """return [k for k in allkeys if kmin <= k <= kmax]"""
        imin = bisect.bisect_left(node.keys, kmin)
        imax = bisect.bisect(node.keys, kmax)

        if node.children:
            for e in node.children[imin:imax+1]:
                for k in self._iterkeys(e, kmin, kmax):
                    yield k
        if node.is_leaf():
            for i in xrange(imin, imax):
                yield node.keys[i]

    def values(self, kmin = None, kmax = None):
        values = []

        if self.is_empty():
            return []

        if kmin is None:
            kmin = self.min()
        if kmax is None:
            kmax = self.max()

        return self._values(self.root, kmin, kmax, values)

    def _values(self, node, kmin, kmax, values):
        """return [v for k in allkeys if kmin <= k <= kmax]"""
        imin = bisect.bisect_left(node.keys, kmin)
        imax = bisect.bisect(node.keys, kmax)

        if node.children:
            for e in node.children[imin:imax+1]:
                self._values(e, kmin, kmax, values)
        if node.is_leaf():
            values.extend(node.values[imin:imax])

        return values

    def itervalues(self, kmin = None, kmax = None):
        if kmin is None:
            kmin = self.min()
        if kmax is None:
            kmax = self.max()

        return self._itervalues(self.root, kmin, kmax)

    def _itervalues(self, node, kmin, kmax):
        """return [k for k in allkeys if kmin <= k <= kmax]"""
        imin = bisect.bisect_left(node.keys, kmin)
        imax = bisect.bisect(node.keys, kmax)

        if node.children:
            for e in node.children[imin:imax+1]:
                for v in self._itervalues(e, kmin, kmax):
                    yield v
        if node.is_leaf():
            for i in xrange(imin, imax):
                yield node.values[i]

    def items(self, kmin = None, kmax = None):
        items = []

        if kmin is None:
            kmin = self.min()
        if kmax is None:
            kmax = self.max()

        return self._items(self.root, kmin, kmax, items)

    def _items(self, node, kmin, kmax, items):
        """return [(k,v) for k in allkeys if kmin <= k <= kmax]"""
        imin = bisect.bisect_left(node.keys, kmin)
        imax = bisect.bisect(node.keys, kmax)

        if node.children:
            for e in node.children[imin:imax+1]:
                self._items(e, kmin, kmax, items)
        if node.is_leaf():
            items.extend(zip(node.keys[imin:imax], node.values[imin:imax]))

        return items

    def iteritems(self, kmin = None, kmax = None):
        if kmin is None:
            kmin = self.min()
        if kmax is None:
            kmax = self.max()

        return self._iteritems(self.root, kmin, kmax)

    def _iteritems(self, node, kmin, kmax):
        """return [k for k in allkeys if kmin <= k <= kmax]"""
        imin = bisect.bisect_left(node.keys, kmin)
        imax = bisect.bisect(node.keys, kmax)

        if node.children:
            for e in node.children[imin:imax+1]:
                for i in self._iteritems(e, kmin, kmax):
                    yield i
        if node.is_leaf():
            for i in xrange(imin, imax):
                yield (node.keys[i], node.values[i])

    def is_empty(self):
        return self.root.is_leaf() and len(self.root.keys) == 0

    def min(self):
        node = self.root
        while node.children:
            node = node.children[0]
        return node.keys[0]

    def max(self):
        node = self.root
        while node.children:
            node = node.children[-1]
        return node.keys[-1]

    def bft(self, node, level = 1):
        """Breadth first traversal."""
        q = Queue.Queue()
        level = level
        q.put((level, node))

        while not q.empty():
            level, node = q.get()
            yield (level, node)
            for e in node.children:
                q.put((level+1, e))

    def levels(self):
        leveldict = {}

        for level, node in self.bft(self.root):
            leveldict.setdefault(level, []).append(node)

        return leveldict

    def pprint(self, width = 80):
        leveldict = self.levels()
        keys = leveldict.keys()
        for k in keys:
            print ' '.join(str(e) for e in leveldict[k]).center(width)

    def __setitem__(self, k, v):
        self.insert(k, v)

    def __getitem__(self, k):
        node, i = self.search(self.root, k)
        if node:
            return node.values[i]
        else:
            return None

    #def __delitem__(self, k):
    #    self._delete(self.root, k)

def test_BPTree():
    b = BPTree(2)
    kv = [
        (0, 'zero'),
        (8, 'eight'),
        (9, 'nine'),
        (1, 'one'),
        (7, 'seven'),
        (2, 'two'),
        (6, 'six'),
        (3, 'three'),
        (5, 'five'),
        (4, 'four'),
        (10, 'ten'),
        (11, 'eleven'),
    ]
    for k, v in kv:
        b[k] = v
    b.pprint()
    n,i = b.search(b.root, 0)
    while n.next:
        print n.next
        n = n.next
    del b[11]
    del b[1]
    del b[2]
    del b[3]
    del b[9]
    b.pprint()
    del b[4]
    b.pprint()
    print b[10]
    print 'min key: ', b.min()
    print 'max key: ', b.max()
    print 'ceiling: ', b.ceiling(b.root, 7.4)
    print 'keys                :', b.keys()
    print 'iterkeys()          :', list(b.iterkeys())
    print 'keys(min, max)      :', b.keys(3.4, 7.9)
    print 'iterkeys(min, max)  :', list(b.iterkeys(3.4, 7.9))
    print 'values()            :', b.values()
    print 'itervalues()        :', list(b.itervalues())
    print 'values(min, max)    :', b.values(3.4, 7.9)
    print 'itervalues(min, max):', list(b.itervalues(3.4, 7.9))
    print 'items()             :', b.items()
    print 'iteritems()         :', list(b.iteritems())
    print 'items(min, max)     :', b.items(3.4, 7.9)
    print 'iteritems(min, max) :', list(b.iteritems(3.4, 7.9))

#################################### END #######################################

if __name__ == '__main__':
    test_BPTree()