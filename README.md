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
    
    Walks a filesystem tree grabbing the necessary information and creating the corresponding Elasticsearch documents to push.

+ Usage:
  - `tim <root_path> <path_to_index_root> <system_id> <api_server> <token> <refresh_token>`
+ Args:
  - root_path
    * Path to start traversing for indexing.  
  - path_to_index_root
    * Path to remove from full filepath. If the files that are being indexed live in a local subdirectory and you want to treat this local subdirectory as the index root folder, the part of the path to delete hsould be specified.
  - system_id
    * Agave system id to use if there's a need to move/rename any file.
  - api_server
    * Api Server URL to use with agavepy.      
  - token
    * Agave OAuth token to use with agavepy.
  - refresh_token       
    * Agave OAuth refresh token to use with agavepy.

## tim-create
    
    Creates a new index using the same settings/mapping as another, specified, indexed.

+ Usage:
  - `tim-create <from> <new_index>`  
+ Args:
  - from
    * Index/Alias name to copy settings/mappings from.
  - new_index
    * New index name to use.

## tim-backup
    
    Backup a specified index using bulk operations.

+ Usage:
  - `tim-backup <from_index> <backup_index> <doc_type> <props_to_exclue>`
+ Args:
  - from_index
    * Index/Alias name to copy data from.
  - backup_index
    * Index name to copy data to.
  - doc_type
    * Document type to copy.
  - props_to_exclude
    * Document's properties to exclude when copying information. Properties used by default:  ('_index', '_parent', '_percolate', '_routing', '_timestamp','_ttl', '_type', '_version', '_version_type', '_id', '_retry_on_conflict'). If you want to exclude any of these you can give a comma separated list. Usually `_id` is used.
    
