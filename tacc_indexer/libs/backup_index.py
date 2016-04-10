from elasticsearch import Elasticsearch, helpers 
from elasticsearch.compat import string_types

def usage():
    print '<from_index> <backup_index> <doc_type> <props_to_exclue>.\nfrom_index: Index/Alias name to copy data from.\nbackup_index: Index name to copy data into. This index should exist already and setup if you don\'t want ES to figure things out automatically.\ndoc_type: Document type to copy.\nprops_to_exclude: Document\'s properties to exclude when copying information. Properties used by default: [_index, _parent, _percolate, _routing, _timestamp, _ttl, _type, _version, _version_type, _id, _retry_on_conflict] you can exclude any of this. Usually _id is used.'

def make_expand(props):
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
    return expand_action

def main(settings):
    props = ('_index', '_parent', '_percolate', '_routing', '_timestamp',
            '_ttl', '_type', '_version', '_version_type', '_id', '_retry_on_conflict')
    if settings.backuper.props_to_exclue is not None:
	    props = tuple(p for p in props if p not in settings.backuper.props_to_exclue)

    es = Elasticsearch(settings.hosts)
    print 'copying from {} to {}, documents {}'.format(settings.backuper.from_index, settings.backuper.index, settings.backuper.doc_type)

    success, failed = helpers.reindex(es, settings.backuper.from_index, settings.backuper.index, scan_kwargs={'doc_type': settings.backuper.doc_type}, bulk_kwargs = {'expand_action_callback': make_expand(props)})

    print 'Documents copied: {}. Documents Failed {}'.format(success, failed)
