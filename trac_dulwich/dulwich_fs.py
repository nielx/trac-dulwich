from trac.core import *
from trac.util.datefmt import FixedOffset, to_timestamp, format_datetime
from trac.versioncontrol.api import \
     Changeset, Node, Repository, IRepositoryConnector, NoSuchChangeset, NoSuchNode

import dulwich.diff_tree
from dulwich.objects import Blob, Commit, Tree
from dulwich.repo import Repo

from datetime import datetime
from StringIO import StringIO

# Utils from TracGit

def _last_iterable(iterable):
    "helper for detecting last iteration in for-loop"
    i = iter(iterable)
    v = i.next()
    for nextv in i:
        yield False, v
        v = nextv
    yield True, v


class DulwichConnector(Component):
    implements(IRepositoryConnector)
    
    def __init__(self):
        self.log.info("Dulwich plugin loaded")
        pass
    
    # IRepositoryConnector
    def get_supported_types(self):
        #Perhaps one day add or change to support 'git'
        yield ("dulwich", 8)
    
    def get_repository(self, type, directory, params):
        assert type =="dulwich"
        return DulwichRepository(directory, params, self.log)

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

    def get_node(self, path, rev=None):
        if not rev:
            rev = self.dulwichrepo.head()
        return DulwichNode(self, path, rev)
    
    def get_oldest_rev(self):
        # Get the oldest rev there is (relative to the current head)
        return self.dulwichrepo.revision_history(self.dulwichrepo.head())[-1].id

    def get_youngest_rev(self):
        return self.dulwichrepo.head()

    def previous_rev(self, rev, path=''):
        if len(path) > 0:
            # TODO: fix this: it currently gets the revision in which it was last changed,
            # not the previuos version...
            node = self.get_node(path, rev)
            return node.get_last_change(rev, path)
        try:
            return self.dulwichrepo.revision_history(rev)[1].id
        except KeyError:
            return None

    def next_rev(self, rev, path=""):
        # TODO: implement (needs database though)
        return None
           
    def normalize_path(self, path):
        return path and path.strip('/') or '/'
    
    def rev_older_than(self, rev1, rev2):
        if not rev1 or not rev2:
            return False
        commit1 = self.dulwichrepo[rev1]
        return commit1 in self.dulwichrepo.revision_history(rev2)
        
    def get_path_history(self, path, rev=None, limit=None):
        raise NotImplementedError
    
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
        
    def get_changes(self, old_path, old_rev, new_path, new_rev,
                        ignore_ancestry=1):
        raise NotImplementedError

    
class DulwichChangeset(Changeset):
    def __init__(self, repo, rev):
        if rev not in repo.dulwichrepo:
            raise NoSuchChangeset(rev)
        
        self.dulwichrepo = repo.dulwichrepo
        self.rev = rev
        message = self.dulwichrepo[rev].message
        author =  self.dulwichrepo[rev].author
        timezonestring = self.dulwichrepo[rev].author_timezone
        timezone = FixedOffset(int(timezonestring)/60, timezonestring)
        date = datetime.fromtimestamp(float(self.dulwichrepo[rev].author_time), timezone)
        Changeset.__init__(self, repo, rev, message, author, date)

    # Constants for get_changes
    CHANGE_TYPES = { dulwich.diff_tree.CHANGE_ADD: Changeset.ADD,
                     dulwich.diff_tree.CHANGE_COPY: Changeset.COPY,
                     dulwich.diff_tree.CHANGE_RENAME: Changeset.MOVE,
                     dulwich.diff_tree.CHANGE_MODIFY: Changeset.EDIT,
                     dulwich.diff_tree.CHANGE_DELETE: Changeset.DELETE }
                     
    KIND_TYPES =   { dulwich.objects.Tree: Node.DIRECTORY ,
                     dulwich.objects.Blob: Node.FILE }
                     
    def get_changes(self):
        """Generator that produces a tuple for every change in the changeset.

        The tuple will contain `(path, kind, change, base_path, base_rev)`,
        where `change` can be one of Changeset.ADD, Changeset.COPY,
        Changeset.DELETE, Changeset.EDIT or Changeset.MOVE,
        and `kind` is one of Node.FILE or Node.DIRECTORY.
        The `path` is the targeted path for the `change` (which is
        the ''deleted'' path  for a DELETE change).
        The `base_path` and `base_rev` are the source path and rev for the
        action (`None` and `-1` in the case of an ADD change).
        """
        # get the changes to the previous revision...
        # The parent is determined through the Commit.parents list: we always use the first
        # TODO: fix for the first revision
        previous_rev = None
        previous_rev = self.dulwichrepo[self.rev].parents[0]
        
        changes = dulwich.diff_tree.tree_changes(self.dulwichrepo.object_store,
                                                 self.dulwichrepo[previous_rev].tree,
                                                 self.dulwichrepo[self.rev].tree)
            
        for change in changes:
            yield(change.new.path, self.KIND_TYPES[self.dulwichrepo[change.new.sha].__class__], 
                  self.CHANGE_TYPES[change.type], 
                  change.old.path if not change.type == dulwich.diff_tree.CHANGE_ADD else None,
                  previous_rev if not change.type == dulwich.diff_tree.CHANGE_ADD else None)
                    

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
                    plop
                    raise NoSuchNode(path, rev)
            # finally we should have an object in self.dulwichobject
            if isinstance (self.dulwichobject, Tree):
                kind = Node.DIRECTORY
                path += '/'
            elif isinstance(self.dulwichobject, Blob):
                kind = Node.FILE
            else:
                raise TracError("Weird kind of Dulwich object for " + path)
        
        rev = self.get_last_change(rev, path)   
        
        #required by the Node class to set up ourselves
        self.created_path = path 
        self.created_rev = rev   # not really true though. TODO: need to fix this with caching?  
        
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
    
    def get_history(self, limit=None):
        # get the backward history for this node
        # TODO: follow moves/copies
        commits = self.dulwichrepo.revision_history(self.rev)
        if self.path == "/":
            # We are getting the history of the root, which is in every commit
            if limit:
                commits = commits[0:limit]
            for is_last, commit in _last_iterable(commits):
                yield (self.path, commit.id, Changeset.EDIT if not is_last else Changeset.ADD)
        else:
            history = []
            elements = self.path.strip('/').split('/')
            # TODO: this code is also used in _get_last_change. Combine and make
            # much nicer. It can probably also be reused in DulwichRepository.get_path_history
            for commit in commits:
                currentobject = self.dulwichrepo.tree(commit.tree)
                refsha = self.dulwichobject.id
                found = False
                for element in elements:
                    # iterate through the tree
                    found = False
                    for name, mode, sha in currentobject.items():
                        if name == element:
                            currentsha = sha
                            currentobject = self.dulwichrepo[sha]
                            found = True
                            break
                    if not found:
                        # This means that the current revision of the object is the right one.
                        break

                # at this point we either found the object with the same name or we didn't
                if found and currentsha == refsha:
                    pass # no change
                elif found:
                    history.append(commit)
                    currentsha = refsha
                else: 
                    #not found
                    break
            for is_last, commit in _last_iterable(history):
                yield(self.path, commit.id, Changeset.EDIT if not is_last else Changeset.ADD)
                
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
        
    # Dulwich specific
    def get_last_change(self, rev, path):
        # Find the last change for the given path since a specified rev
        elements = path.strip('/').split('/')
        commits = self.dulwichrepo.revision_history(rev)
        refsha = self.dulwichobject.id
        
        for commit in commits:
            currentobject = self.dulwichrepo.tree(commit.tree)

            found = False
            for element in elements:
                # iterate through the tree
                found = False
                for name, mode, sha in currentobject.items():
                    if name == element:
                        currentsha = sha
                        currentobject = self.dulwichrepo[sha]
                        found = True
                        break
                if not found:
                    # This means that the current revision of the object is the right one.
                    return rev
                    
            # at this point we either found the object with the same name or we didn't
            if found and currentsha == refsha:
                rev = commit.id
            elif found:
                return rev
        
        raise TracError("Unknown error in TracDulwich (_get_last_change)")
        