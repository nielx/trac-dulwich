from trac.admin import AdminCommandError, IAdminCommandProvider
from trac.core import *
from trac.util.text import printout
from trac.versioncontrol import RepositoryManager

import dulwich.repo
import dulwich.objects

import os.path
import sys


######
# The command line tools
######

class DulwichCacheAdmin(Component):
    """trac-admin command provider for permission system administration."""
     
    implements(IAdminCommandProvider)
 
    def get_admin_commands(self):
        yield ('dulwich sync', '<project>',
                'Synchronize a repository cache',
                None, self._do_sync)
     
    def _do_sync(self, reponame):
        rm = RepositoryManager(self.env)
        repos = rm.get_repository(reponame)
        if repos is None:
            raise TracError("Repository '%(repo)s' not found", repo=reponame)

        printout("Synchronizing repository data for repository %s" % 
            (reponame,))

        db = self.env.get_db_cnx()
        cursor = db.cursor()
        
        # The database stores the heads up to what it has currently cached. Use
        # these heads to determine where to stop to only cache the new
        # revisions
        exclude_list = []
        cursor.execute("SELECT head FROM dulwich_heads WHERE repos=%s", 
                       (repos.id,))
        
        for head in set(row[0] for row in cursor):
            exclude_list.append(head)
        
        # Determine all the heads for this repository
        heads = []
        refs = repos.dulwichrepo.get_refs()
        for key in refs.keys():
            if key.startswith("refs/heads/"):
                heads.append(refs[key])
        
        commit_count = 0
        object_count = 0
        
        walker = repos.dulwichrepo.get_walker(include=heads, 
                                              exclude=exclude_list)
        for walk in walker:
            for change in walk.changes():
                parents = []
                if isinstance(change, list):
                    # The change is a list when the file is a merge from two 
                    # or more previous changesets
                    for c in change:
                        if c.old not in parents:
                            parents.append(c.old)
                    change = change[0]
                else:
                    parents.append(change.old)
                
                if change.type == "delete":
                    # we don't actually register deletes, they are registered 
                    # when they are last modified
                    continue

                # check if this object is already in the database
                cursor.execute("SELECT commit_id FROM dulwich_objects WHERE repos=%s AND sha=%s", (repos.id, change.new.sha))
                item = cursor.fetchone()
                
                if item:
                    try:
                        cursor.execute("UPDATE dulwich_objects SET commit_id=%s WHERE repos=%s AND sha=%s", (walk.commit.id, repos.id, change.new.sha))
                        db.commit()
                    except:
                        # Todo: this is probably all right and has to do with merge changesets, but need to
                        # verify if it is absolutely the way it should be
                        pass
                else:
                    # in case of add, or a modify of a file that we did not yet encounter (because
                    #   we run in reverse order)
                    cursor.execute("INSERT INTO dulwich_objects (repos, sha, path, mode, commit_id) VALUES (%s, %s, %s, %s, %s)",
                                (repos.id, change.new.sha, change.new.path.decode("utf-8"), change.new.mode, walk.commit.id))
                    db.commit()
                    object_count += 1
                    
                if change.type == "add":
                    # above in fetching o we already update the commit_id, so no action here
                    pass
                elif change.type == "modify":
                    for parent in parents:
                        try:
                            # actually the commit_id for the old changeset is wrong, but it will be updated in the following runs of the loop
                            cursor.execute("INSERT INTO dulwich_objects (repos, sha, path, mode, commit_id) VALUES (%s, %s, %s, %s, %s)",
                                     (repos.id, parent.sha, parent.path.decode("utf-8"), parent.mode, walk.commit.id))
                            db.commit()
                            object_count += 1
                        except: 
                            # if this fails, it means that the parent object is already in the database
                            # very likely because of merges. So it is safe to ignore. 
                            pass
                        

                # handle the trees
                path = os.path.split(change.new.path)[0]
                if not path:
                    continue
                current_path = ""
                tree = repos.dulwichrepo[walk.commit.tree]
                for part in path.split('/'):
                    # register each tree into the object store
                    current_path += part
                    mode, sha = tree.lookup_path(repos.dulwichrepo.get_object, current_path)
                    try:
                        cursor.execute("INSERT INTO dulwich_objects (repos, sha, path, mode, commit_id) VALUES (%s, %s, %s, %s, %s)",
                                 (repos.id, sha, current_path.decode("utf-8"), mode, walk.commit.id))
                        db.commit()
                        object_count += 1
                    except:
                        # this tree was already registered with a previous path change
                        pass
                    current_path += '/'
            # prepare for next run
            commit_count += 1
            if commit_count % 5 == 0:
                sys.stdout.write('Synchronized %i commits with %i objects\r' % 
                                 (commit_count, object_count))
                sys.stdout.flush()
                
        # Store the heads
        cursor.execute("DELETE FROM dulwich_heads WHERE repos=%s", (repos.id,))
        for head in heads:
            cursor.execute("""INSERT INTO dulwich_heads (repos, head)  
                           VALUES (%s, %s)
                           """, (repos.id, head))
        db.commit()
        printout('Synchronized %i commits with %i objects' %
                 (commit_count, object_count))
        


#####
# Classes used by repositories
#####

class DulwichCache(object):
    def __init__(self, repos, log, repos_id, env):
        self.repos = repos
        self.logger = log
        self.env = env
                
    def exists(self, sha):
        db = self.env.get_db_cnx()
        cursor = db.cursor()
        cursor.execute("SELECT sha FROM dulwich_objects WHERE repos=%s", (self.repos.id,))
        sha = cursor.fetchone()
        if sha:
            return sha[0]
        else:
            return None
                
    def get_commit_sha_for_object(self, sha):
        db = self.env.get_db_cnx()
        cursor = db.cursor()
        cursor.execute("SELECT commit_id FROM dulwich_objects WHERE repos=%s AND sha=%s", (self.repos.id, sha))
        item = cursor.fetchone()
        if item:
            self.logger.debug("Fetching object %s from cache!" % (sha))
            return item[0]
        else:
            self.logger.info("Object %s not in cache!" % (sha))
            return None
