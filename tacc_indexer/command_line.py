from tacc_indexer.libs.indexer import Indexer
from tacc_indexer.libs import create_index  as ci_util
from tacc_indexer.libs import backup_index as bi_util
import sys
import getopt

def index():
    argv = sys.argv[1:]
    if len(argv) < 3:
        usage()
        return
    elif len(argv) > 3 and len(argv) < 6:
        usage()
        return

    if len(argv) == 3:
        indexer = Indexer(argv[0], argv[1], argv[2])
    else:
        indexer = Indexer(argv[0], argv[1], argv[2], argv[3], argv[4], argv[5])
    indexer.index()

def create_index():
    argv = sys.argv[1:]
    if len(argv) < 2:
        ci_util.usage()
        return
    
    ci_util.main(argv)

def backup_index():
    argv = sys.argv[1:]
    if len(argv) < 3:
        bi_util.usage()
        return
    
    bi_util.main(argv)

def usage():
    print('<root_path> <path_to_index_root> <system_id> <api_server> <token> <refresh_token>\nroot_path: Path to start traversing for indexing.\npath_to_index_root: Path to remove from full filepath. If the files that are being indexed live in a local subdirectory this parameter should specify the part of the path to be deleted so the files can be indexed as if they existed in /.\nsystem_id: Remote system id to work on.\napi_server: Agave server api URL\ntoken: Token to create an Agave Client.\nrefresh_token: Refresh token for the Agave Client.')

