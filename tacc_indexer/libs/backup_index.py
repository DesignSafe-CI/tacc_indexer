from elasticsearch import Elasticsearch, helpers 

props = ('_index', '_parent', '_percolate', '_routing', '_timestamp',
            '_ttl', '_type', '_version', '_version_type', '_id', '_retry_on_conflict')

def usage():
    print '<from_index> <backup_index> <doc_type> <props_to_exclue>.\nfrom_index: Index/Alias name to copy data from.\nbackup_index: Index name to copy data into. This index should exist already and setup if you don\'t want ES to figure things out automatically.\ndoc_type: Document type to copy.\nprops_to_exclude: Document\'s properties to exclude when copying information. Properties used by default: [_index, _parent, _percolate, _routing, _timestamp, _ttl, _type, _version, _version_type, _id, _retry_on_conflict] you can exclude any of this. Usually _id is used.'


def expand_action(data):
    """
    From one document or action definition passed in by the user extract the
    action/data lines needed for elasticsearch's
    :meth:`~elasticsearch.Elasticsearch.bulk` api.
    """
    # when given a string, assume user wants to index raw json
    if isinstance(data, string_types):
        return '{"index":{}}', data

    # make sure we don't alter the action
    data = data.copy()
    op_type = data.pop('_op_type', 'index')
    action = {op_type: {}}
    for key in props:
        if key in data:
            action[op_type][key] = data.pop(key)

    # no data payload for delete
    if op_type == 'delete':
        return action, None

    return action, data.get('_source', data)

def main(argv):
    if len(argv) >= 4:
	    props = tuple(p for p in props if p not in argv[3].split(','))

    es = Elasticsearch(['designsafe-es01.tacc.utexas.edu', 'designsafe-es02.tacc.utexas.edu'])
    helpers.reindex(es, argv[0], argv[1], scan_kwargs={'doc_type': argv[2]}, bulk_kwargs = {'expand_action_callback': expand_action})