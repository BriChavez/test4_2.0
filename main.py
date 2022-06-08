import pandas as pd
from glob import glob
import logging 
import sys
import warnings
from logging import INFO
from sqlalchemy import MetaData, Table, Column, String, Numeric, DateTime
import sqlalchemy as sa

# setup logging and logger
logging.basicConfig(format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s',
                    level=INFO,
                    stream=sys.stderr)
logger: logging.Logger = logging

class DataLoader():
    def __init__(self, filepath, index = None) -> None:
        """load a file into a csv"""
        df = pd.read_csv(filepath, header = 0)
        # set index if specified
        if index is not None:
            df = pd.set_index(index)
        self.df = df

    def head(self) -> None:
        """prints the head of the dataframe to console"""
        print(self.df.head())

    def info(self):
        # utility to expose the info method of our data frame onto our base class
        """bind info to our dataframe"""
        return self.df.info

    def add_index(self, index_name="index", col_list="list") -> None:
        """create an index column"""
        # if our data doesn't already have one, this is a function that creates an index
        df = self.df
        # summon our logger buddy so he can later let us know when we make an index and what we named him
        logger.info(f"\tAdding index {index_name}")
        # concats cols in col_list into an index column to serve us faithfully as our loyal primary key
        df[index_name] = df[col_list].apply(lambda row: "-".join(row.values.astype(str)), axis=1)
        # replaces the index of the dataframe to our concated columns
        df.set_index(index_name, inplace=True)
        # this gives us the ability to add an index when one is not present
        self.df = df

    def sort(self, column_name:str) -> None:
        """sort the data by a later specified column"""
        df = self.df
        df.sort_values(column_name)

    def merge(self, df, left_on, right_on, join_cols, how = 'left'):
        """merge df from multiple CSVs on later specified columns"""
        df = pd.merge(left = self.df, right = df[join_cols], left_on = left_on, right_on = right_on, how = how)
        

    def load_to_db(self, db_table_name:str) -> None:
        """load dataframe into a database table"""
        self.df.to_sql(name=db_table_name, con=self.engine, if_exists='replace', chunksize=2000)


def db_engine(db_host: str, db_user: str, db_pass: str, db_name: str = "spotify") -> sa.engine.Engine:
        """use SqlAlchemy to create a database engine and return it"""
        # create engine using SqlAlchmey
        engine = sa.create_engine(f'mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}', future = True)
        # binds metadata to the engine
        metadata = MetaData(bind=engine)
        # sets the connect to the engine to a variable to call upon later
        conn = engine.connect()
        return engine


def db_create_tables(db_engine, drop_first = False) -> None:
    """create tables """
    meta = sa.MetaData(bind=db_engine)
    # define columns from the artists table
    artists_table = Table("artists", meta,
                        Column('artist_popularity', Numeric),
                        Column('followers', Numeric),
                        Column('genres', String(10240)),
                        Column('id', Numeric, primary_key=True),
                        Column('name', String(256)),
                        Column('track_id', String(256)),
                        Column('track_name_prev', String(256)),
                        Column('type', String(256)),
                        extend_existing=True)

    # define columns from the albums table
    albums_table = Table("albums", meta,
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
                        extend_existing=True)

    # drop tables if drop_first = True
    if drop_first:
        meta.drop_all()

    # create tables
    meta.create_all(checkfirst=True)
    

def main():
    """Pipeline Orchestratation method."""
    # create a data loader for albums
    album_data = DataLoader('./data/spotify_albums.csv')
    # create a data loader for artists
    artist_data = DataLoader('./data/spotify_artists.csv')
    # print the first 10 rows of album set
    album_data.head()
    # print the first 10 rows of artist set
    artist_data.head()
    # set index of albums to three of the columns
    album_data.add_index('index', ['artist_id', 'id', 'release_date'])
    # set index of artists to id
    artist_data.add_index('id', ['id'])
    # sort artist by name
    artist_data.sort('name')
    # create db engine
    engine = db_engine(db_host="127.0.0.1:3306", db_user="root", db_pass="mysql")
    # create db meta date table
    db_create_tables(engine, drop_first = True)
    # load artists into a db
    artist_data.load_to_db(engine, 'artists')
    # load albums into a db
    album_data.load_to_db(engine, 'albums')
    # merge them both into one super table
    artist_data.merge(df = artist_data, left_on = artist_data, right_on = album_data, join_cols = 'id', sort = 'id', how = 'left')


if __name__ == '__main__':
    main()