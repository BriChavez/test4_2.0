import pandas as pd
from glob import glob
import logging 
import sys
import warnings
from logging import INFO

# setup logging and logger
logging.basicConfig(format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s',
                    level=INFO,
                    stream=sys.stderr)
logger: logging.Logger = logging

class DataLoader():
    def __init__(self, filepath, index = None, multi_file_load = False) -> None:
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
        # concats cols in col_list into an index column to serve us faithfully as our loyal primary key
        df[index_name] = df[col_list].apply(lambda row: "-".join(row.values.astype(str)), axis=1)
        df.set_index(index_name, inplace=True)
        self.df = df
        # this gives us the ability to add an index when one is not present

    def sort(self, column_name:str) -> None:
        """sort the data by a later specified column"""
        df = self.df
        df.sort_values(column_name)
        self.df = df

    def load_to_db(self, db_engine, db_table_name:str) -> None:
        """load dataframe into a database table"""
        df = self.df
        self.df.to_sql(name=db_table_name, con=self.engine, if_exists='replace', chunksize=2000)
        self.df = df


def db_engine(db_host: str, db_user: str, db_pass: str, db_name: str = "spotify") -> sa.engine.Engine:
    """Using SqlAlchemy, create a database engine and return it"""
    #create enginge using sqlalchmey
    engine = create_engine(
        f'mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}', future = True)
    metadata = MetaData(bind=engine)
    conn = engine.connect()
    return engine


def db_create_tables(db_engine, drop_first:bool = False) -> None:
    meta = sa.MetaData(bind=db_engine)


    #define columns from the artists table
    artists_table = Table("artists", metadata,
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
    albums_table = Table("albums", metadata,
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
    # album_type,artist_id,available_markets,external_urls,href,id,images,name,release_date,release_date_precision,total_tracks,track_id,track_name_prev,uri,type

    #drop tables is drop_first = True
    if drop_first:
        metadata.drop_all()

    #create tables
    metadata.create_all(checkfirst=True)
        meta = sa.MetaData(bind=db_engine)

        # your code to define tables go in here
        #   - Be careful, some of the columns like album.available_markets are very long. Make sure you give enough DB length for these. ie: 10240 (10kb)

        # your code to drop and create tables go here


def main():
    """
    Pipeline Orchestratation method.

    Performs the following:
    - Creates a DataLoader instance for artists and albums
    - prints the head for both instances
    - Sets artists index to id column
    - Sets albums index to artist_id, name, and release_date
    - Sorts artists by name
    - creates database engine
    - creates database metadata tables/columns
    - loads both artists and albums into database
    """
    pass


if __name__ == '__main__':
    main()