from os.path import split, join, getsize, isdir
from os import walk, stat
from datetime import datetime
from tacc_indexer.libs.elasticsearch import Object
from tacc_indexer.libs.agave import AgaveManager
import sys
import getopt
import re
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
"""
# Use this if you want to log to a file
LOG_FILENAME = 'path/to/file/name'
handler = logging.handlers.TimedRotatingFileHandler(LOG_FILENAME, when='D', interval = 1, backupCount = 5)
"""
# Handler to log to std.err
handler = logging.StreamHandler()

formatter = logging.Formatter('[INDEXER] %(levelname)s %(asctime)s %(module)s %(name)s.%(funcName)s:%(lineno)s : %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


AGAVE_FILESYSTEM = 'designsafe.storage.default'

def usage():
    print('<root_path> <path_to_index_root> <system_id> <api_server> <token> <refresh_token>\nroot_path: Path to start traversing for indexing.\npath_to_index_root: Path to remove from full filepath. If the files that are being indexed live in a local subdirectory this parameter should specify the part of the path to be deleted so the files can be indexed as if they existed in /.\nsystem_id: Remote system id to work on.\napi_server: Agave server api URL\ntoken: Token to create an Agave Client.\nrefresh_token: Refresh token for the Agave Client.')

class Indexer(object):
    def __init__(self, start_path, del_path, system_id, api_server, token, refresh_token):
        self.api_server = api_server
        self.token = token
        self.refresh_token = refresh_token
        self.system_id = system_id
        self.start_path = start_path
        self.del_path = del_path
        self.mgr = AgaveManager(api_server = api_server, token = token, refresh_token = refresh_token)

    def get_index_obj(self, filepath, filename):
        fn = join(filepath, filename)
        mime_type = 'application/octet-stream'
        name = filename
        format = 'raw'
        deleted = False
        last_modified = stat(fn).st_ctime
        file_type = filename.split('.')[-1]
        agave_path = 'agave://{}/{}'.format(AGAVE_FILESYSTEM, fn.replace(self.del_path, '', 1).strip('/'))
        system_tags = []
        length = getsize(fn)
        system_id = AGAVE_FILESYSTEM
        path = filepath.replace(self.del_path, '', 1).strip('/')
        keywords = []
        type = 'file'
        if isdir(fn):
            #print 'is Dir'
            mime_type = 'text/directory'
            type = 'dir'
            format = 'folder'
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

    def save_obj(self, o):
        o = Object(**o)
        o.save()

    def del_obj(self, path, name):
        #print 'Searching for path: {}, name: {}'.format(path, name)
        o = Object.get_exact_filepath(AGAVE_FILESYSTEM, path, name, self.del_path)
        print '-' + join(o['path'], o['name'])
        o.delete()

    def index_files(self, root, files_to_index):
        #print 'objs to index'
        for name in files_to_index:
            o = self.get_index_obj(root, name)
            print '+' + join(o['path'], o['name'])
            self.save_obj(o)
            #o.save()

    def delete_indexed(self, root, files_to_delete):
        #print 'filenames to delete'
        for name in files_to_delete:
            self.del_obj(root, name)

    def remove_duplicates(self, indexed_objs):
        seen = set()
        ret = []
        #print 'len inexed_objs {}'.format(len(indexed_objs))
        for o in indexed_objs:
            if o.name in seen:
                print '-' + join(o.path, o.name)
                o.delete()
            else:
                seen.add(o.name)
                ret.append(o)
        return ret

    def check_deleted(self, deleted_objs):
        home_dir = deleted_objs[0].path.split('/')[0]
        trash = Object.get_exact_filepath(AGAVE_FILESYSTEM, home_dir, '.Trash')
        if trash is None:
            trash = Object(**{
                'mimeType': 'text/directory',
                'name': '.Trash',
                'format': 'folder',
                'deleted': False,
                'lastModified': datetime.now().isoformat(),
                'fileType': 'folder',
                'length': 32768,
                'agavePath': 'agave://{}/{}'.format(AGAVE_FILESYSTEM, join(home_dir, '.Trash')),
                'systemId': AGAVE_FILESYSTEM,
                'path': home_dir,
                'type': 'dir',
            })
            trash.save()
            logger.info('Created .Trash {}'.format(trash.to_dict()))
            self.mgr.mkdir(AGAVE_FILESYSTEM, home_dir, '.Trash')
        for o in deleted_objs:
            o.update(oldPath = join(o.path, o.name))
            if Object.get_exact_filepath(AGAVE_FILESYSTEM, join(trash.path, trash.name), o.name) is not None:
                new_name = o.name + '_' + datetime.now().isoformat().replace(':', '-')
                logger.info('renaming to: {}'.format(new_name))
                renamed = self.mgr.rename(AGAVE_FILESYSTEM, join(o.oldPath), new_name)
                if renamed:
                    o.update(name = new_name)
                else:
                    return

            moved = self.mgr.move(AGAVE_FILESYSTEM, join(o.path, o.name), join(trash.path, trash.name))
            if moved: 
                o.update(path = join(trash.path, trash.name), deleted = True)
            else:
                return

            o.update(agavePath = 'agave://{}/{}'.format(AGAVE_FILESYSTEM, join(o.path, o.name)))
            logger.info('Object moved into trash {}'.format(o.to_dict()))
            if o.format == 'folder':
                res, s = Object.search_exact_path(AGAVE_FILESYSTEM, o.oldPath)
                cnt = 0
                if res.hits.total:
                    while cnt <= res.hits.total - len(res):
                        for obj in s[cnt:cnt + len(res)]:
                            regex = r'^{}'.format(o.oldPath)
                            obj.update(path = re.sub(regex, join(o.path, o.name), obj.path, count = 1), deleted = True, oldPath = join(obj.path, obj.name))
                            obj.update(agavePath = 'agave://{}/{}'.format(AGAVE_FILESYSTEM, join(obj.path, obj.name)))
                            logger.info('Object moved into trash {}'.format(obj.to_dict()))
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
        return files_to_index, files_to_delete

    def is_home_dir(self, folder):
        path = folder.replace(self.del_path, '', 1)
        path, name = split(path.strip('/'))
        ret = len(path.split('/')) == 1
        #logger.info('is_home_dir: {}, path: {}'.format(ret, path))
        return ret
            
    def index(self):
        logger.info('initializing: {}'.format(self.start_path))
        for root, dirs, files in walk(self.start_path):
            if self.is_home_dir(root):
                dirs[:] = [d for d in dirs if d != '.Trash']
            res, s = Object.search_exact_path(AGAVE_FILESYSTEM, root, self.del_path)
            indexed_objs = [o for o in s.scan()]
            indexed_filenames = [o.name for o in indexed_objs if o.name != '.Trash']
            fs_filenames = dirs + files
            self.process_filenames(root, fs_filenames, indexed_filenames)
            indexed_objs = self.remove_duplicates(indexed_objs)
            objs_to_delete = [o for o in indexed_objs if o.deleted]
            if len(objs_to_delete) >= 1:
                self.check_deleted(objs_to_delete)
                dirs[:] = [d for d in dirs if d not in [nd.name for nd in objs_to_delete]]
                #logger.info('dirs: {}'.format(dirs))
            if root == self.start_path:
                logger.info('Done with first level, fire up permissions')

#def main(argv):
#    if len(argv) < 6:
#        usage()
#        return
#
#    start_path = argv[0]
#    del_path = argv[1]
#    indexer = Indexer(argv[0], argv[1], argv[2], argv[3], argv[4], argv[5])
#    indexer.index()
#
#if __name__ == '__main__':
#    main(sys.argv[1:])
