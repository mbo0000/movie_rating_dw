from movie_api import MovieAPI
import time
import pandas as pd

from nyt_article_ingestor import NYTIngestor

# 1,000 daily limit

# TODOS: check for date last update, if not today, then exit
class OMDBIngestor:

    def __init__(self): 
        self.key    = MovieAPI.get_key('OMDB')
        self.path   = MovieAPI.project_path

    def get_nyt_movie_title(self, path:str) -> list:
        '''
        Read NYT csv file and return list of movies title

        :param path: file path dir
        :type path: str
        :return: list
        '''

        with open(path + '/data_processed/nyt.csv', 'r') as file:
            return list(pd.read_csv(file)['movie_title'])

    def omdb_process_data(self):
        '''
        API calls and processing data from OMDB using NYT movie data
        '''

        movies  = []

        # ommit last update timestamp
        for title in self.get_nyt_movie_title(self.path):
            
            if title == '' or not isinstance(title, str):
                continue
            
            params = {
                't': title.strip()
                , 'type':'movie'
                , 'apikey': self.key
            }
            
            response = MovieAPI.make_api_request('OMDB', params=params)

            movie = {}
            movie['imdbID']     = response['imdbID']
            movie['title']      = response['Title']
            movie['runtime']    = response['Runtime']
            movie['genre']      = response['Genre'].split(', ')
            movie['director']   = response['Director']
            movie['actor']      = response['Actors'].split(', ')
            movie['writer']     = response['Writer'].split(', ')
            movie['language']   = response['Language'].split(', ')
            movie['released']   = response['Released']
            movie['country']    = response['Country'].split(', ')
            movie['plot']       = response['Plot']
            movie['imdbVotes']  = response['imdbVotes']
            movie['box_office'] = response['BoxOffice']
            movie['award']      = response['Awards']

            if ratings := response['Ratings']:
                movie.update({rating['Source']: rating['Value'] for rating in ratings})


            movies.append(movie)
            time.sleep(1.5)
        
        df = pd.DataFrame(movies)
        df.to_csv(path_or_buf = self.path+'/data_processed/omdb.csv')

