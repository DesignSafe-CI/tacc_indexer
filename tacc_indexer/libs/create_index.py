from elasticsearch import Elasticsearch
import sys

def usage():
    print '<from> <new_index> Create new index from an existing index, copying _mappings and _settings'

def get_data_from_index(es, index_name):
    iname = index_name
    if es.indices.exists_alias('', iname):
        alias = es.indices.get_alias('', iname)
        alias_name = alias.iteritems().next()[0]
        iname = alias_name

    index = es.indices.get(iname, '_settings,_mappings')
    return index.iteritems().next()[1]

def get_analyzers(index):
    settings = index['settings']['index']
    analysis = None
    if 'analysis' in settings:
        analysis = {'analysis': settings.pop('analysis')}
    return analysis

def main(settings):
    es = Elasticsearch(settings.hosts)
    index = get_data_from_index(es, settings.creator.from_index)
    analyzers = get_analyzers(index)
    mappings = index['mappings']
    #print 'Analyzers: {}'.format(analyzers)
    #print 'Mapping: {}'.format(mappings)
    
    if es.indices.exists(settings.creator.index):
        r = 'Y'
        if not settings.creator.yes:
            r = raw_input('%s index exists, do you want to delete it? [Y/n]' % settings.creator.index)
        if r.lower() != 'n':
            if settings.verbosity:
                sys.stdout.write('new_index: {} exists. Deleting...\n'.format(settings.creator.index))
            es.indices.delete(settings.creator.index)
    else:
        if settings.verbosity:
            sys.stdout.write('new_index: {} dosn\'t exists\n'.format(i_to))
    es.indices.create(settings.creator.index, {'settings': analyzers, 'mappings': mappings})
