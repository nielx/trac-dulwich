from trac.core import *
from trac.util.datefmt import FixedOffset, to_timestamp, format_datetime
from trac.versioncontrol.api import \
     Changeset, Node, Repository, IRepositoryConnector, NoSuchChangeset, NoSuchNode

from dulwich.objects import Blob, Commit, Tree
from dulwich.repo import Repo

from datetime import datetime
from StringIO import StringIO

class DulwichConnector(Component):
    implements(IRepositoryConnector)
    
    def __init__(self):
        self.log.info("Dulwich plugin loaded")
        pass
    
    # IRepositoryConnector
    def get_supported_types(self):
        #Perhaps one day add or change to support 'git'
        yield ("dulwich", 8)
    
    def get_repository(self, type, dir, params):
        assert type =="dulwich"
        return DulwichRepository(dir, params, self.log)

class DulwichRepository(Repository):
    def __init__(self, path, params, log):
        self.params = params
        self.path = path
        self.logger = log
        self.dulwichrepo = Repo(path)
        Repository.__init__(self, "dulwich:"+path, self.params, log)
    
    def close(self):
        self.dulwichrepo = None
    
    def get_changeset(self, rev):
        return DulwichChangeset(self, rev)
    
    def get_youngest_rev(self):
        return self.dulwichrepo.head()
    
    def previous_rev(self, rev, path=''):
        if path != None:
            raise NotImplementedError
        try:
            return self.dulwichrepo.revision_history(rev)[1].id
        except KeyError:
            return None
    
    def next_rev(self, rev, path=""):
        # TODO: implement (needs database though)
        return None
    
    def get_node(self, path, rev=None):
        if not rev:
            rev = self.dulwichrepo.head()
        return DulwichNode(self, path, rev)
    
    def get_oldest_rev(self):
        raise NotImplementedError
    
    def normalize_path(self, path):
        return path and path.strip('/') or '/'
    
    def normalize_rev(self, rev):
        if not rev:
            return self.dulwichrepo.head()
        test = self.dulwichrepo.get_object(rev)
        if not isinstance(test, Commit):
            raise NoSuchChangeset(rev)
        return rev 
    
    def short_rev(self, rev):
        #NOTE: This should actually verify whether the names clash. At the other hand
        #      this is never used programmatically, and users running into problems
        #      should be skilled enought to work it out themselves.
        return rev[0:7]
    
    def display_rev(self, rev):
        return self.short_rev(rev)

    
class DulwichChangeset(Changeset):
    def __init__(self, repo, rev):
        self.dulwichrepo = repo.dulwichrepo
        self.rev = rev
        message = self.dulwichrepo[rev].message
        author =  self.dulwichrepo[rev].author
        timezonestring = self.dulwichrepo[rev].author_timezone
        timezone = FixedOffset(int(timezonestring)/60, timezonestring)
        date = datetime.fromtimestamp(float(self.dulwichrepo[rev].author_time), timezone)
        Changeset.__init__(self, repo, rev, message, author, date)


class DulwichNode(Node):
    def __init__(self, repos, path, rev, sha=None):
        self.dulwichrepo = repos.dulwichrepo
        if sha == None and path == "/":
            # get the tree
            self.dulwichobject = self.dulwichrepo.tree(self.dulwichrepo[rev].tree)
            kind = Node.DIRECTORY
        elif sha:
            self.dulwichobject = self.dulwichrepo.get_object(sha)
            if isinstance(self.dulwichobject, Tree):
                kind = Node.DIRECTORY
            else:
                kind = Node.FILE
        else:
            # walk the tree
            elements = path.strip('/').split('/')
            self.dulwichobject = self.dulwichrepo.tree(self.dulwichrepo[rev].tree)
            for element in elements:
                # verify that our current object is a tree
                if not isinstance(self.dulwichobject, Tree):
                    raise NoSuchNode(path, rev)
                
                # run through the tree
                found = False
                for rubbish, name, sha in self.dulwichobject.entries():
                    if name == element:
                        found = True
                        self.dulwichobject = self.dulwichrepo.get_object(sha)
                        break
                if not found:
                    raise NoSuchNode(path, rev)
            # finally we should have an object in self.dulwichobject
            if isinstance (self.dulwichobject, Tree):
                kind = Node.DIRECTORY
                path += '/'
            elif isinstance(self.dulwichobject, Blob):
                kind = Node.FILE
            else:
                raise TracError("Weird kind of Dulwich object for " + path)
        Node.__init__(self, repos, path, rev, kind)
    
    def get_content(self):
        if not self.isfile:
            return None
        return StringIO(self.dulwichobject.as_raw_string())

    def get_entries(self):
        if not self.isdir:
            return
        
        for rubbish, name, sha in self.dulwichobject.entries():
            yield DulwichNode(self.repos, self.path + name, self.rev, sha)

    def get_properties(self):
        # no properties defined yet...
        return {}
    
    def get_content_type(self):
        if self.isdir:
            return None
        # git does no accounting
        return ''

    def get_content_length(self):
        if self.isdir:
            return None
        return self.dulwichobject.raw_length()