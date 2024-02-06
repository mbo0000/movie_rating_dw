
import json
from datetime import datetime, timedelta
import time
import re
import math
import pandas as pd
from movie_api import MovieAPI

# ====================================================================================================
# =================================== NYT ============================================================
# ====================================================================================================

# https://developer.nytimes.com/apis
# 500 requests per day and 5 requests per minute. You should sleep 12 seconds between calls to avoid hitting the per minute rate limit. 

class NYTIngestor:
    
    def __init__(self):
        self.key    = MovieAPI.get_key('NYT')
        self.path   = MovieAPI.project_path

    def reformat_name(self, names: list) -> list:
        '''
        Split each name at ", " and format name to first last order with optional suffixes
        
        :param val: list of names
        :type val: list
        :return: list
        '''
        format_names = []
        for name in names:
            suffix      = re.search(r'^\w ([ISJr]+)', name).groups()[0] if re.search(r'^\w ([ISJr]+)', name) else ''
            suffix_rm   = name.replace(suffix, '')
            parts       = [part.strip() for part in suffix_rm.split(', ') if part]
            
            if len(parts) > 1:
                full_name = f'{parts[1]} {parts[0]} {suffix}'
            else:
                full_name = name

            format_names.append(full_name.strip())
        return format_names


    def remove_char(self, val:list) -> list:
        '''
        remove characters in start/end with and between ()
        
        :param val: list of name
        :type val: list
        :return: list
        '''
        if len(val) == 0:
            return ['']
        
        elif isinstance(val, list):    
            return [re.sub(r' \(.+\)', '', name) for name in val]


    def nyt_process_data(self, data:json) -> dict:
        for kw in data['response']['docs']:
            
            if kw.get('section_name') == 'Movies':
                article = {}
                article['movie_title']          = self.remove_char([v['value'] for v in kw['keywords'] if v['name'] == 'creative_works'])[0]
                article['people']               = self.reformat_name(self.remove_char([v['value'] for v in kw['keywords'] if v['name']=='persons']))
                article['article_title']        = kw['headline']['main']
                article['critic_pick']          = kw['headline']['kicker']
                article['web_url']              = kw['web_url']
                article['pub_date']             = kw['pub_date']
                article['reporter_first_name']  = [v['firstname'] for v in kw['byline']['person']][0]
                article['reporter_last_name']   = [v['lastname'] for v in kw['byline']['person']][0]
                yield article



    def nyt_ingestion(self):
        now = datetime.now()

        # start and end date of the current week
        start_of_week   = now - timedelta(days=now.weekday())
        end_of_week     = start_of_week + timedelta(days=6)
        page            = 0
        movies          = []
        endpoint       = 'articlesearch.json?'

        while True:

            # per documentation: nyt rate limit 5 calls per minute            
            time.sleep(12)
            params = {
                'begin_date' : start_of_week.strftime("%Y%m%d")
                , 'end_date' : end_of_week.strftime("%Y%m%d")
                , 'q' : 'movies'
                , 'fq' : 'type_of_material:(\"Review\")'
                , 'page' : page
                , 'api-key': self.key
            }
            
            response = MovieAPI.make_api_request('NYT', endpoint, params)

            if response != None:
                for item in self.nyt_process_data(response):
                    item['last_updated'] = now
                    movies.append(item)

            # pagination: starting page = 0
            # check if more data are available, 10 hits a page, so 43 hits = 4 pages starting from 0
            if math.floor(response['response']['meta']['hits'] / 10) > page:
                page += 1
                continue
            
            break

        df = pd.DataFrame(movies)
        df.to_csv(path_or_buf = self.path+'/data_processed/nyt.csv')
