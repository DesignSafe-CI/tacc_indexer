from agavepy.agave import Agave, AgaveException
from requests.exceptions import ConnectionError, HTTPError   
from os.path import join, split
import urllib
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
"""
# Use this if you want to log to a file
LOG_FILENAME = 'path/to/file/name'
handler = logging.handlers.TimedRotatingFileHandler(LOG_FILENAME, when='D', interval = 1, backupCount = 5)
"""
# Handler to log to std.err
handler = logging.StreamHandler()

formatter = logging.Formatter('[INDEXER] %(levelname)s %(asctime)s %(module)s %(name)s.%(funcName)s:%(lineno)s : %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class AgaveManager(object):
    def __init__(self, api_server, token, refresh_token):
        self.agave_client = Agave(api_server = api_server, 
                                  token = token,
                                  refresh_token = refresh_token)
    
    def set_owner_pems(self, system_id, path):
        owner = path.split('/')[0]
        if not len(owner):
            logger.error('Couldn\'t figure out the owner of: {}'.format(path))
            return False
        
        args = {
            'systemId': system_id,
            'filePath': urllib.quote(path),
            'body': '''{{ "recursive": "true", 
                        "permission": "{}",
                        "username": "{}"
                      }}'''.format('ALL', owner)
        }
        try:
            self.agave_client.files.updatePermissions(**args)
        except (AgaveException, HTTPError) as e:
            logger.error('Error {} when trying to update the permissions using this args: {}'.format(e.message, args), exc_info = True)
            return False

        return True

    def mkdir(self, system_id, path, name):
        logger.info('Creating folder: {}'.format(path + '/' + name))
        args = {
            'systemId': system_id,
            'filePath': urllib.quote(path),
            'body': '{{ "action": "mkdir", "path": "{}" }}'.format(name)
        }
        try:
            self.agave_client.files.manage(**args)
            self.set_owner_pems(system_id, path + '/' + name)
        except (AgaveException, HTTPError) as e:
            logger.error('Error {} trying to mkdir using this args: {}'.format(e.message, args), exc_info = True)

    def move(self, system_id, path, new_path):
        new_path = urllib.unquote(new_path)
        p, n = split(path)
        args = {
            'systemId': system_id,
            'filePath': urllib.quote(path),
            'body': { "action": "move", "path": join(new_path, n)}
        }
        try:
            self.agave_client.files.manage(**args)
            return True
        except (AgaveException, HTTPError) as e:
            logger.error('Error {} trying to move using this args: {}'.format(e.message, args), exc_info = True)
            return False

    def rename(self, system_id, path, new_name):
        path = urllib.unquote(path)
        args = {
            'systemId': system_id,
            'filePath': path,
            'body': { "action": "rename", "path": new_name }
        }
        try:
            self.agave_client.files.manage(**args)
            return True
        except (AgaveException, HTTPError) as e:
            logger.error('Error {} trying to rename using this args: {}'.format(e.message, args), exc_info = True)
            return False
