import pandas as pd
from glob import glob
import logging 
import sys
import warnings
from logging import INFO
import sqlalchemy as sa
from sqlalchemy import MetaData, Table, Column, String, Numeric, DateTime

# setup logging and logger
logging.basicConfig(format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s',
                    level=INFO,
                    stream=sys.stderr)
logger: logging.Logger = logging

class DataLoader():
    def __init__(self, filepath, index = None) -> None:
        df = pd.read_csv(filepath, header = 0)
    # load a file into a csv
    # set index if specified
        if index is not None:
            df = pd.set_index(index)
        self.df = df

    def head(self) -> None:
        """prints the head of the dataframe to console"""
        print(self.df.head())

    # utility to expose the info method of our data frame onto our base class
    def info(self):
        """bind info to our dataframe"""
        return self.df.info

        # if our data doesn't already have one, this is a function that creates an index from a concat of columns as a list as well is an optional name for index
    def add_index(self, col_list, index_name="index") -> None:
        """create an index column by concating s list of columns into a string"""
        df = self.df
        # summon our logger buddy so he can later let us know when we make an index and what we named him
        logger.info(f"\tAdding inex {index_name}")
        self.df = df
        # this gives us the ability to add an index when one is not present

    def sort(self, column_name:str) -> None:
        """sort the data by a later specified column"""
        df = self.df
        df.sort_values(column_name)
        self.df = df

    def merge(self, df, left_on, right_on, join_cols, how = 'left'):
        # merge df from multiple csvs on later specified columns
        df = self.df
        df = pd.merge(left = self.df, right = df[join_cols], left_on = left_on, right_on = right_on, how = how)
        

    def load_to_db(self, db_engine, db_table_name:str) -> None:
        """load dataframe into a database table"""
        df = self.df
        self.df.to_sql(name=db_table_name, con=self.engine, if_exists='replace', chunksize=2000)
        self.df = df


def db_engine(db_host: str, db_user: str, db_pass: str, db_name: str = "spotify") -> sa.engine.Engine:
        """Using SqlAlchemy, create a database engine and return it"""
        #create enginge using sqlalchmey
        engine = sa.create_engine(f'mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}', future = True)
        metadata = MetaData(bind=engine)
        return engine


def db_create_tables(db_engine, drop_first = False) -> None:
    meta = sa.MetaData(bind=db_engine)
    #define columns from the artists table
    artists_table = Table("artists",
                        Column('artist_poularity', Numeric),
                        Column('followers', Numeric),
                        Column('genres', String(10240)),
                        Column('id', Numeric, primary_key=True),
                        Column('name', String(256)),
                        Column('track_id', String(256)),
                        Column('track_name_prev', String(256)),
                        Column('type', String(256)),
                        extend_existing=True
                        )
    # ,artist_popularit~y,followers,genres,id,name,track_id,track_name_prev,type
    #define columns from the albums table
    albums_table = Table("albums",
                        Column('album_type', String(256)),
                        Column('artist_id', String(256)),
                        Column('available_markets', String(10240)),
                        Column('external_urls', String(256)),
                        Column('href', String(256)),
                        Column('id', String(256), primary_key=True),
                        Column('images', String(10340)),
                        Column('name', String(10240)),
                        Column('release_date', DateTime, nullable=True),
                        Column('release_date_precision', String(256)),
                        Column('total_tracks', Numeric),
                        Column('track_id', String(256)),
                        Column('track_name_prev', String(256)),
                        Column('uri', String(256)),
                        Column('type', String(256)),
                        extend_existing=True
                        )

    #drop tables if drop_first = True
    if drop_first:
        meta.drop_all()

    #create tables
    meta.create_all(checkfirst=True)
    


def main():
    """Pipeline Orchestratation method."""
    # create a data loader for albums and artists
    album_data = DataLoader('./data/spotify_albums.csv')
    artist_data = DataLoader('./data/spotify_artists.csv')
    # print the first 10 rows of each set
    album_data.head()
    artist_data.head()
    # set index of albums to three of the columns
    album_data.add_index('index', ['artist_id', 'id', 'release_date'])
    # set index of artists to id
    artist_data.add_index('id', ['id'])
    # sort artist by name
    artist_data.sort('name')
    # create db engine
    engine = db_engine('127.0.0.1:3306', 'root', 'mysql')
    # create db meta date table
    db_create_tables(engine, drop_first = True)
    # load them both into a db
    artist_data.load_to_db(engine, 'artists')
    album_data.load_to_db(engine, 'albums')
    # merge them both into one super table
    artist_data.merge(df = artist_data, left_on = artist_data, right_on = album_data, join_cols = 'id', sort = 'id', how = 'left')


if __name__ == '__main__':
    main()