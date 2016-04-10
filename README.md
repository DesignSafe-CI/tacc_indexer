# Indexer 

Tools to manage file index between Agave and Elasticsearch.

# Installation

Installation, for now, it's done by cloning this repo and installing is in dev mode using pip.

```
    $ git clone https://github.com/DesignSafe-CI/tacc_indexer.git
    $ cd tacc_indexer
    $ pip install -e .
```

# Commands

There are multiple commands in this tool. Every command is prefixed with `tim` (TACC Index Manager)

## tim

usage: tim [-h] [-v] [--config CONFIG] [-H [HOSTS [HOSTS ...]]]
           [-a API_SERVER] [-t TOKEN] [-r REFRESH_TOKEN] [-i INDEX] [-d DOC]
           root_path path_to_index_root system_id

Indexer to traverse a filesystem directly and create elasticsearch documents
to index

positional arguments:

-  root_path             Path to start traversing for indexing.
-  path_to_index_root    Path to handle as '/'. This means that it will be removed from the full filepath.
-  system_id             System Id to use when creating the documents.

optional arguments:

-  -h, --help            show this help message and exit
-  -v, --verbosity       increases output verbosity
-  --config CONFIG       Specify config file. If every argument is listed inthe config file then it is not necessary to specify it in the command line.
-  -H [HOSTS [HOSTS ...]], --hosts [HOSTS [HOSTS ...]] One or more hosts to use to connect to ElasticSearch
-  -a API_SERVER, --api_server API_SERVER Api Server URL to use with agavepy
-  -t TOKEN, --token TOKEN Token to use with agavepy.
-  -r REFRESH_TOKEN, --refresh_token REFRESH_TOKEN Agave OAuth refresh token to use with agavepy
-  -i INDEX, --index INDEX ES Index name to use. Defaults to "testing".
-  -d DOC, --doc DOC     ES doc_type name to use. Defaults to "objects".

## tim-create

usage: tim-create [-h] [-v] [--config CONFIG] [-H [HOSTS [HOSTS ...]]] [-Y]
                  from_index index

Creates an empty index copying settings and mappings from another given index

positional arguments:

-  from_index            Index/Alias from where to copy settings and mappings.
-  index                 Index to create and copy settings and mapping to. Note: It will drop and recreate the index if it already exists.

optional arguments:

-  -h, --help            show this help message and exit
-  -v, --verbosity       increases output verbosity
-  -H [HOSTS [HOSTS ...]], --hosts [HOSTS [HOSTS ...]] One or more hosts to use to connect to ElasticSearch
-  -Y, --yes             If set every prompt will automatically be respondedwith Yes

## tim-backup

usage: tim-backup [-h] [-v] [--config CONFIG] [-H [HOSTS [HOSTS ...]]]
                  from_index index doc_type
                  [props_to_exclude [props_to_exclude ...]]

Copy all data from one given index to another

positional arguments:

-  from_index            Index/Alias from where to copy data.
-  index                 Index to copy data into. This should be an existent index if you don't want ES to setup things automatically.
-  doc_type              Document type to copy.
-  props_to_exclude      List of the properties to exclude when copying the documents

optional arguments:

-  -h, --help            show this help message and exit
-  -v, --verbosity       increases output verbosity
-  --config CONFIG       Specify config file. If every argument is listed in the config file then it is not necessary to specify it in the command line.
-  -H [HOSTS [HOSTS ...]], --hosts [HOSTS [HOSTS ...]]
                        One or more hosts to use to connect to ElasticSearch

## Config File

The config file data is only used by `tim` and `tim-backup`. The property `indexer` configures the command `tim` and the property `backuper` configures `tim-backup`.

    {
        "hosts": ["http://designsafe-es01.tacc.utexas.edu:9200/", "http://designsafe-es01.tacc.utexas.edu:9200/"],
        "indexer": {
                "root_path": "/Users/xirdneh/indexer_test",
                "path_to_index_root": "/Users/xirdneh/indexer_test",
                "system_id": "designsafe.storage.default",
                "api_server": null,
                "token": null,
                "refresh_token": null,
                "index": "tester",
                "doc": "objects"
            },
        "backuper": {
            "from_index": "tester",
            "index": "designsafe_backup",
            "doc_type": "objects",
            "props_to_exclue": ["_id" ]
        }
    }
