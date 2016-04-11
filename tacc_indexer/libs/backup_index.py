from elasticsearch import Elasticsearch, helpers 
from elasticsearch.compat import string_types
import sys

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
    if settings.backuper.props_to_exclude is not None:
	    props = tuple(p for p in props if p not in settings.backuper.props_to_exclude)

    es = Elasticsearch(settings.hosts)
    if settings.verbosity:
        sys.stdout.write('copying from {} to {}, documents {}\n'.format(settings.backuper.from_index, settings.backuper.index, settings.backuper.doc_type))

    success, failed = helpers.reindex(es, settings.backuper.from_index, settings.backuper.index, scan_kwargs={'doc_type': settings.backuper.doc_type}, bulk_kwargs = {'expand_action_callback': make_expand(props)})
    if settings.verbosity:
        sys.stdout.write('Documents copied: {}. Documents Failed {}\n'.format(success, failed))
