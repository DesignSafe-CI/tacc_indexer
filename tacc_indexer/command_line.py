from tacc_indexer.libs.indexer import Indexer
import sys
import getopt

def tindexer():
    argv = sys.argv[1:]
    indexer = Indexer(argv[0], argv[1], argv[2], argv[3], argv[4], argv[5])
    indexer.index()
