from setuptools import setup

setup(name = 'tacc_fs_indexer',
      version = '0.1',
      description = 'Tools to walk filesystem and create indexable objects',
      url = 'http://github.com/indexer',
      author = 'Josue Balandrano Coronel',
      author_email = 'jcoronel@tacc.utexas.edu',
      license = 'MIT',
      packages = ['tacc_fs_indexer'],
      zip_safe = false,
      install_requires = [
        'agavepy'
        'elasticsearch',
        'elasticsearch_dsl'
      ],
      #test_suite = 'nose.collector',
      #test_require = ['nose'],
      )
