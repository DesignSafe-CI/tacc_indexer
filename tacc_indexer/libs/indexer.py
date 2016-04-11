from os.path import split, join, getsize, isdir
from os import walk, stat
from datetime import datetime
from tacc_indexer.libs.es_api import ESManager, Obj
from tacc_indexer.libs.agave import AgaveManager
import elasticsearch
import sys
import re

AGAVE_FILESYSTEM = 'designsafe.storage.default'

class Indexer(object):
    def __init__(self, start_path, del_path, system_id, api_server = None, token = None, refresh_token = None, hosts = None, verbosity = False, **kwargs):
        self.api_server = api_server
        self.token = token
        self.refresh_token = refresh_token
        self.system_id = system_id
        self.start_path = start_path
        self.del_path = del_path
        self.mgr = None
        self.esm = None
        self.verbosity = verbosity
        self.added_cnt = 0
        self.del_cnt = 0
        self.doc_type = getattr(kwargs, 'doc_type', None)
        if api_server is not None:
            self.mgr = AgaveManager(api_server = api_server, token = token, refresh_token = refresh_token)
        if hosts is not None:
            self.esm = ESManager(hosts, **kwargs)

    def get_index_obj(self, filepath, filename):
        fn = join(filepath, filename)
        mime_type = 'application/octet-stream'
        name = filename
        format = 'raw'
        deleted = False
        last_modified = stat(fn).st_ctime
        file_type = filename.split('.')[-1]
        agave_path = 'agave://{}/{}'.format(self.system_id, fn.replace(self.del_path, '', 1).strip('/'))
        system_tags = []
        length = getsize(fn)
        system_id = self.system_id
        path = filepath.replace(self.del_path, '', 1).strip('/')
        keywords = []
        type = 'file'
        if isdir(fn):
            #print 'is Dir'
            mime_type = 'text/directory'
            type = 'dir'
            format = 'folder'
            file_type = 'folder'
        if path == '.' or path == '':
            path = '/'
        return {
            'mimeType': mime_type,
            'name': name,
            'format': format,
            'deleted': deleted,
            'lastModified': datetime.fromtimestamp(last_modified).isoformat(),
            'fileType': file_type,
            'agavePath': agave_path,
            'systemTags': system_tags,
            'length': length,
            'systemId': system_id,
            'path': path,
            'keywords': keywords,
            'type': type
        }

    def index_files(self, root, files_to_index):
        #print 'objs to index'
        for name in files_to_index:
            o = self.get_index_obj(root, name)
            if self.verbosity:
                sys.stdout.write("indexing \t %s \t %s\n" % (join(root, name), join(o['path'], o['name'])))
            self.esm.save(o)
            self.added_cnt += 1

    def delete_indexed(self, root, files_to_delete):
        #print 'filenames to delete'
        for name in files_to_delete:
            o = self.esm.get_exact_filepath(self.system_id, root, name, self.del_path)
            if self.verbosity:
                sys.stdout.write("deleting \t -- \t %s\n" % join(o.path, o.name))
            self.esm.delete(o)
            self.del_cnt += 1

    def remove_duplicates(self, indexed_objs):
        seen = set()
        ret = []
        #print 'len inexed_objs {}'.format(len(indexed_objs))
        for o in indexed_objs:
            if o.name in seen:
                self.esm.delete(o)
                if self.verbosity:
                    sys.stdout.write("deleting \t -- \t %s\n" % join(o.path, o.name))
            else:
                seen.add(o.name)
                ret.append(o)
        return ret

    def check_deleted(self, deleted_objs):
        home_dir = deleted_objs[0].path.split('/')[0]
        trash = self.esm.get_exact_filepath(self.system_id, home_dir, '.Trash')
        if trash is None:
            trash_doc = {
                'mimeType': 'text/directory',
                'name': '.Trash',
                'format': 'folder',
                'deleted': False,
                'lastModified': datetime.now().isoformat(),
                'fileType': 'folder',
                'length': 32768,
                'agavePath': 'agave://{}/{}'.format(self.system_id, join(home_dir, '.Trash')),
                'systemId': self.system_id,
                'path': home_dir,
                'type': 'dir',
            }
            sret = self.esm.save(trash_doc)
            trash = Obj.from_es(self.esm.get(sret))
            self.mgr.mkdir(self.system_id, home_dir, '.Trash')
            if self.verbosity:
                sys.stdout.write('indexing \t %s \t %s\n' % (join(home_dir, '.Trash'), join(trash.path, trash.name)))
        for o in deleted_objs:
            self.esm.update(o.meta.id, oldPath = join(o.path, o.name))
            if self.esm.get_exact_filepath(self.system_id, join(trash.path, trash.name), o.name) is not None:
                new_name = o.name + '_' + datetime.now().isoformat().replace(':', '-')
                renamed = self.mgr.rename(self.system_id, o.oldPath, new_name)
                if renamed:
                    if self.verbosity:
                        sys.stdout.write('renaming \t %s -> %s \t --\n' % (o.oldPath, new_name))
                    self.esm.update(o.meta.id, name = new_name)
                else:
                    return

            moved = self.mgr.move(self.system_id, join(o.path, o.name), join(trash.path, trash.name))
            if moved: 
                self.esm.update(o.meta.id, path = join(trash.path, trash.name), deleted = True)
                self.esm.update(o.meta.id, agavePath = 'agave://{}/{}'.format(self.system_id, join(o.path, o.name)))
                if self.verbosity:
                    sys.stdout.write('moving \t %s -> %s \t %s -> %s\n' % (o.oldPath, join(trash.path, trash.name, o.name), o.oldPath, join(trash.path, trash.name, o.name)))
            else:
                return

            if o.format == 'folder':
                res, s = self.esm.search_exact_path(self.system_id, o.oldPath)
                cnt = 0
                if res.hits.total:
                    while cnt <= res.hits.total - len(res):
                        for obj in s[cnt:cnt + len(res)]:
                            regex = r'^{}'.format(o.oldPath)
                            old_path = join(obj.path, obj.name)
                            new_path = re.sub(regex, join(o.path, o.name), obj.path, count = 1)
                            self.esm.update(obj.meta.id, path = new_path, deleted = True, oldPath = old_path)
                            self.esm.update(obj.meta.id, agavePath = 'agave://{}/{}'.format(self.system_id, join(obj.path, obj.name)))
                            if self.verbosity:
                                sys.stdout.write("moving \t -- \t %s -> %s\n" % (oldPath, new_path))
                        cnt += len(res)

    def process_filenames(self, root, fs_filenames, indexed_filenames):
        #print 'fs_filenames {}'.format(fs_filenames)
        #print 'indexed_filenames {}'.format(indexed_filenames)
        #get filenames that are not in the index
        files_to_index = [n for n in fs_filenames if n not in indexed_filenames]
        #get filenames that are in the index but not in the filesystem
        files_to_delete = [n for n in indexed_filenames if n not in fs_filenames]
        
        self.index_files(root, files_to_index)

        self.delete_indexed(root, files_to_delete) 
        #return files_to_index, files_to_delete

    def is_home_dir(self, folder):
        path = folder.replace(self.del_path, '', 1)
        path, name = split(path.strip('/'))
        ret = len(path.split('/')) == 1
        #logger.info('is_home_dir: {}, path: {}'.format(ret, path))
        return ret
            
    def index(self):
        if self.verbosity:
            sys.stdout.write("action \t local path \t index path\n")
            sys.stdout.write("----------------------------------\n")
        for root, dirs, files in walk(self.start_path):
            if self.verbosity:
                sys.stdout.write("traversing \t  %s \t %s\n" % (root, root.replace(self.del_path, '', 1)))

            if self.is_home_dir(root):
                dirs[:] = [d for d in dirs if d != '.Trash']

            res, s = self.esm.search_exact_path(self.system_id, root, self.del_path)
            indexed_objs = [o for o in s.scan()]
            indexed_filenames = [o.name for o in indexed_objs if o.name != '.Trash']
            fs_filenames = dirs + files
            self.process_filenames(root, fs_filenames, indexed_filenames)
            indexed_objs = self.remove_duplicates(indexed_objs)
            objs_to_delete = [o for o in indexed_objs if o.deleted]
            if self.mgr is not None and len(objs_to_delete) >= 1:
                self.check_deleted(objs_to_delete)
                dirs[:] = [d for d in dirs if d not in [nd.name for nd in objs_to_delete]]

    def actions(self, index_name, doc_type):
        for root, dirs, files in walk(self.start_path):
            filenames = dirs + files
            docs = []
            docs_names = []
            docs_dup = []
            if self.verbosity:                
                sys.stdout.write("traversing \t  %s \t %s\n" % (root, root.replace(self.del_path, '', 1)))
            res, s = self.esm.search_exact_path(self.system_id, root, self.del_path)
            #print res.hits.total
            if res.hits.total:
                seen = set()
                for d in s.scan():
                    docs.append(d)
                    docs_names.append(d.name)
                    if d.name in seen:
                        docs_dup.append(d)
                    else:
                        seen.add(d.name)
            names_to_index = [n for n in filenames if n not in docs_names]
            docs_to_delete = [d for d in docs if d.name not in filenames]
            for n in names_to_index:
                d = self.get_index_obj(root, n)
                #print '+' + join(root, d['name'])
                yield {
                    '_op_type': 'index',
                    '_index': index_name,
                    '_type': doc_type,
                    '_source': d
                }
            for d in docs_to_delete + docs_dup:
                #print '-' + join(d.path, d.name)
                yield {
                    '_op_type': 'delete',
                    '_index': index_name,
                    '_type': doc_type,
                    '_id': d.meta.id
                }


def main(settings):
    indexer = Indexer(settings.indexer.root_path, 
                      settings.indexer.path_to_index_root, 
                      settings.indexer.system_id, 
                      settings.indexer.api_server, 
                      settings.indexer.token, 
                      settings.indexer.refresh_token, 
                      hosts = settings.hosts,
                      index = settings.indexer.index,
                      doc_type = settings.indexer.doc,
                      verbosity = settings.verbosity)
    if settings.single:
        indexer.index()
        if settings.verbosity:
            sys.stdout.write('Documents Added: {}\nDocuments Deleted: {}\n'.format(indexer.added_cnt, indexer.del_cnt))  
    else:
        success, fail = elasticsearch.helpers.bulk(indexer.esm.es, indexer.actions(settings.indexer.index, settings.indexer.doc))
        if settings.verbosity:
            sys.stdout.write('Success: {}\nErrors: {}\n'.format(success, fail))  
