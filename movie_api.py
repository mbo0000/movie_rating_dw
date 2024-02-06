import configparser
import requests
import os

class MovieAPI:

    # class properties here since these will never change
    config          = configparser.ConfigParser()
    project_path    = os.path.dirname(__file__)
    config.read(project_path + '/config.ini')

    @classmethod
    def make_api_request(cls, api, endpoint='', params = {}):

        url     = cls.config.get(api, 'url') + endpoint
        headers = {'Accept': 'application/json'}

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            return response.json()
        else:
            print(api + " Error:", response.status_code, response.text)
        
        return None
    
    @classmethod
    def get_key(cls, api):
        return cls.config.get(api, 'api_key')
