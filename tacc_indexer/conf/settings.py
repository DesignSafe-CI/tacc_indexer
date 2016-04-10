import json
import six

class Settings(object):
    _config_file = None
    _args = None
    _verbosity = 0
    _doc_type = 'objects'
    index = 'testing'
    doc_type = 'objects'
    hosts = ['http://designsafe-es01.tacc.utexas.edu:9200/', 'http://designsafe-es02.tacc.utexas.edu:9200/']
    indexer = type('Empty', (object,), {})
    creator = type('Empty', (object,), {})
    backuper = type('Empty', (object,), {})

    def set_config_file(self, config_file):
        self._config_file = config_file
        config_json = json.loads(config_file)
        for key, val in six.iteritems(config_json):
            k = getattr(self, key)
            for p, v in six.iteritems(val):
                setattr(k, p, v)

    def set_args(self, name, args):
        for prop, val in six.iteritems(args.__dict__):
            if prop.startswith('_'):
                continue
            k = getattr(self, name)
            setattr(k, prop, val)
        self.index = args.index
        self.doc_type = getattr(args, 'doc_type', None)
        self.hosts = args.hosts    
settings = Settings()
