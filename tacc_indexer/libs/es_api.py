import elasticsearch
import elasticsearch_dsl

class ESManager(object):
    def __init__(self, hosts, sniff_on_start = True, sniff_on_connection_fail = True, sniffer_timeout = 60, retry_on_timeout = True, timeout = 20, index = None, doc_type = None):
        es = elasticsearch.Elasticsearch(hosts, sniff_on_start = sniff_on_start,
                           sniff_on_connection_fail = sniff_on_connection_fail,
                           sniffer_timeout = sniffer_timeout,
                           retry_on_timeout = retry_on_timeout,
                           timeout = timeout)
        self.es = es
        self.index = index
        self.doc_type = doc_type

    def _search(self, q):
        s = elasticsearch_dsl.Search(using = self.es,
                                     index = self.index,
                                     doc_type = self.doc_type)
        s.update_from_dict(q)
        return s.execute(), s

    def _sanitize_path(self, path, del_path):
        ret_path = path
        if del_path is not None:
            ret_path = ret_path.replace(del_path, '', 1).strip('/')
        if ret_path == '.' or ret_path == '':
            ret_path = '/'
        return ret_path

    def search_partial_path(self, system_id, path, del_path = None):
        #print 'Searching in root: {}'.format(path)
        path = self._sanitize_path(path, del_path)
        q = {"query":{
                "bool":{
                    "must":[
                        {"term":{
                            "path._path":path
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
        return self._search(q)

    def search_exact_path(self, system_id, path, del_path = None):
        #print 'Searching in root: {}'.format(path)
        path = self._sanitize_path(path, del_path)
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
        return self._search(q)

    def get_exact_filepath(self, system_id, path, name, del_path = None):
        path = self._sanitize_path(path, del_path)
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
        res, s = self._search(q)                
        if res.hits.total:
            return res[0]
        else:
            return None

    def update(self, doc_id, **fields):
        self.es.update(self.index, self.doc_type, doc_id, {'doc': fields})

    def save(self, doc):
        c = self.es.create(self.index, self.doc_type, doc)
        o = self.es.get(self.index, c['_id'], self.doc_type)
        ret = o
        return ret

    def delete(self, doc):
        self.es.delete(self.index, self.doc_type, doc.meta.id)

class Obj(elasticsearch_dsl.DocType):
    pass
