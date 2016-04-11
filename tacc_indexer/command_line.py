from tacc_indexer.libs import create_index  as ci_util
from tacc_indexer.libs import backup_index as bi_util
from tacc_indexer.libs import indexer as i_util
from tacc_indexer.conf.settings import settings
from time import time
import sys
import argparse

class ArgumentParserError(Exception): pass

class ErrorHandleArgumentParser(argparse.ArgumentParser):
    def error(self, message, print_error = False):
        if not print_error:
            raise ArgumentParserError(message)
        super(ErrorHandleArgumentParser, self).error(message)

parent_parser = ErrorHandleArgumentParser(add_help=False)
parent_parser.add_argument('-v', '--verbosity', action='store_true', default = False, help = 'increases output verbosity', required=False)
parent_parser.add_argument('--config', help = 'Specify config file. If every argument is listed in the config file then it is not necessary to specify it in the command line. See README for a config file example', required=False)
parent_parser.add_argument('-H', '--hosts', help = 'One or more hosts to use to connect to ElasticSearch', nargs='*', required=False, default = ['http://designsafe-es01.tacc.utexas.edu:9200/', 'http://designsafe-es01.tacc.utexas.edu:9200/'])

def index():
    #parent_parser.parse_known_args(sys.argv[1:])
    parser = ErrorHandleArgumentParser(description = 'Indexer to traverse a filesystem directly and create elasticsearch documents to index', parents=[parent_parser])
    parser.add_argument('root_path', help="Path to start traversing for indexing.")
    parser.add_argument('path_to_index_root', help="Path to handle as '/'. This means that it will be removed from the full filepath.")
    parser.add_argument('system_id', help="System Id to use when creating the documents.")
    parser.add_argument('-s', '--single', help="If set bulk operations will not be used. Single operations for each of the documents will be used.", action = 'store_true', default = False)
    parser.add_argument('-a', '--api_server', help="Api Server URL to use with agavepy.")
    parser.add_argument('-t', '--token', help="Token to use with agavepy.")
    parser.add_argument('-r', '--refresh_token', help="Agave OAuth refresh token to use with agavepy.")
    parser.add_argument('-i', '--index', help = 'ES Index name to use. Defaults to "testing".')
    parser.add_argument('-d', '--doc', help = 'ES doc_type name to use. Defaults to "objects".')
    try:
        args = parser.parse_args()
        settings.set_args('indexer', args)
    except ArgumentParserError as e:
        error = e.message
        try:
            args = parent_parser.parse_args(sys.argv[1:])
            if getattr(args, 'config', None) is None:
                parser.error(error, True)
            settings.set_config_file(args.config) 
        except ArgumentParserError as err:
            parser.error(error, True)
    t0 = time()
    i_util.main(settings)
    t1 = time()
    sys.stdout.write('Accurate enough running time: {}\n'.format(t1 - t0))

def create_index():
    parser = ErrorHandleArgumentParser(description = 'Creates an empty index copying settings and mappings from another given index', parents=[parent_parser])
    parser.add_argument('from_index', help="Index/Alias from where to copy settings and mappings.")
    parser.add_argument('index', help="Index to create and copy settings and mapping to.\nNote: It will drop and recreate the index if it already exists.")
    parser.add_argument('-Y', '--yes', action='store_true', help="If set every prompt will automatically be responded with Yes", default = False)
    try:
        args = parser.parse_args()
        settings.set_args('creator', args)
    except ArgumentParserError as e:
        parser.error(e.message, True)
    t0 = time()
    ci_util.main(settings)
    t1 = time()
    sys.stdout.write('Accurate enough running time: {}\n'.format(t1 - t0))

def backup_index():
    parser = ErrorHandleArgumentParser(description = "Copy all data from one given index to another", parents=[parent_parser])
    parser.add_argument('from_index', help="Index/Alias from where to copy data.")
    parser.add_argument('index', help="Index to copy data into. This should be an existent index if you don't want ES to setup things automatically.")
    parser.add_argument('doc_type', help="Document type to copy.")
    parser.add_argument('props_to_exclude', help="List of the properties to exclude when copying the documents", nargs = '*', default=[])
    try:
        args = parser.parse_args()
        settings.set_args('backuper', args)
    except ArgumentParserError as e:
        error = e.message
        try:
            args = parent_parser.parse_args(sys.argv[1:])
            if getattr(args, 'config', None) is None:
                parser.error(error, True)
            settings.set_config_file(args.config)
        except ArgumentParserError as err:
            parser.error(error, True)
    t0 = time()    
    bi_util.main(settings)
    t1 = time()
    sys.stdout.write('Accurate enough running time: {}\n'.format(t1 - t0))
