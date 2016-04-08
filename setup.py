from setuptools import setup

setup(name = 'tacc_indexer',
      version = '0.1',
      description = 'Tools to walk filesystem and create indexable objects as well as managing ES indexes.',
      url = 'http://github.com/indexer',
      author = 'Josue Balandrano Coronel',
      author_email = 'jcoronel@tacc.utexas.edu',
      license = 'MIT',
      packages = ['tacc_indexer'],
      zip_safe = False,
      install_requires = [
        'agavepy',
        'elasticsearch',
        'elasticsearch_dsl'
      ],
      entry_points = {
          'console_scripts': ['tinm=tacc_indexer.command_line:index',
                              'tinm-create=tacc_indexer.command_line:create_index']
      }
      #test_suite = 'nose.collector',
      #test_require = ['nose'],
      )
