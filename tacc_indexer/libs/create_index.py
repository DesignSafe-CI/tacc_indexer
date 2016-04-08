from elasticsearch import Elasticsearch

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

def main(argv):
    i_from = argv[0]
    i_to = argv[1]
    es = Elasticsearch(['http://designsafe-es01.tacc.utexas.edu:9200/', 'http://designsafe-es02.tacc.utexas.edu:9200/'])
    index = get_data_from_index(es, i_from)
    analyzers = get_analyzers(index)
    mappings = index['mappings']
    #print 'Analyzers: {}'.format(analyzers)
    #print 'Mapping: {}'.format(mappings)
    
    if es.indices.exists(i_to):
        print 'new_index: {} exists. Deleting...'.format(i_to)
        es.indices.delete(i_to)
    else:
        print 'new_index: {} dosn\'t exists'.format(i_to)
    es.indices.create(i_to, {'settings': analyzers, 'mappings': mappings})
