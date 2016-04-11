import json
import six

class Settings(object):
    _config_file = None
    _args = None
    _doc_type = 'objects'
    verbosity = False
    single = False
    index = 'testing'
    doc_type = 'objects'
    hosts = ['http://designsafe-es01.tacc.utexas.edu:9200/', 'http://designsafe-es02.tacc.utexas.edu:9200/']
    indexer = type('Empty', (object,), {})
    creator = type('Empty', (object,), {})
    backuper = type('Empty', (object,), {})

    def set_config_file(self, config_file):
        self._config_file = config_file
        with open(self._config_file) as f:
            config_json = json.load(f)
        for key, val in six.iteritems(config_json):
            k = getattr(self, key)
            if isinstance(val, dict):
                for p, v in six.iteritems(val):
                    setattr(k, p, v)
            else:
                setattr(self, key, val)

        indexer = getattr(config_json, 'indexer', None)
        if indexer is not None:
            self.index = getattr(indexer, 'index', None)
            self.doc_type = getattr(indexer, 'doc_type', None)
        
    def set_args(self, name, args):
        for prop, val in six.iteritems(args.__dict__):
            if prop.startswith('_'):
                continue
            k = getattr(self, name)
            setattr(k, prop, val)
        self.index = args.index
        self.doc_type = getattr(args, 'doc_type', None)
        self.hosts = args.hosts
        self.verbosity = args.verbosity
        self.single = getattr(args, 'single', None)

settings = Settings()
