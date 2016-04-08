import elasticsearch
import elasticsearch_dsl

elasticsearch_dsl.connections.connections.configure(
    default = {
        'hosts': ['http://designsafe-es01.tacc.utexas.edu:9200', 'http://designsafe-es02.tacc.utexas.edu:9200'],
        'sniff_on_start': True,
        'sniff_on_connection_fail': True,
        'sniffer_timeout': 60,
        'retry_on_timeout': True,
        'timeout': 20,
    })

class Object(elasticsearch_dsl.DocType):
    @classmethod
    def search_exact_path(cls, system_id, path, del_path = None):
        if del_path is not None:
            path = path.replace(del_path, '', 1).strip('/')
        if path == '.' or path == '':
            path = '/'
        #print 'Searching in root: {}'.format(path)
        q = {"query":{
                "bool":{
                    "must":[
                        {"term":{
                            "path._exact":path
                            }
                        }, 
                        {"term": {
                            "systemId": system_id
                            }
                        }
                    ]
                    }
                }
            }
        s = cls.search()
        s.update_from_dict(q)
        return s.execute(), s

    @classmethod
    def get_exact_filepath(cls, system_id, path, name, del_path = None):
        if del_path is not None:
            path = path.replace(del_path, '', 1).strip('/')
        if path == '.' or path == '':
            path = '/'
        q = {"query":{
                "bool":{
                    "must":[
                        {"term":{
                            "path._exact":path
                            }
                        },
                        {"term":{
                            "name._exact":name
                            }
                        },
                        {"term": {
                            "systemId": system_id
                            }
                        }
                    ]
                    }
                }
            }
        s = cls.search()
        s.update_from_dict(q)
        s = s.execute()
        if s.hits.total:
            return s[0]
        else:
            return None
    def save(self, using=None, index=None, validate=True, **kwargs):
        #print 'Searching for path: {}, name: {}'.format(self.path, self.name)
        o = self.__class__.get_exact_filepath(self.path, self.name, '')
        #print 'search result: {}'.format(o.to_dict())
        if o is not None:
            o.update(**o.to_dict())
            return True
        else:
            return super(Object, self).save(using = using, index = index, validate = validate, **kwargs)

    class Meta:
        index = 'testing'
        doc_type = 'objects'

