 Code Outline for main.py
"""
Starter code. Finish implementing the methods in this code
"""
import pandas as pd
import sqlalchemy as sa


class DataLoader():

    
    def __init__(self, filepath:str, index=None, multi_file_load=False) -> None:
    # if statement to load files and set index
    
    if multi_file_load:
        # load multiple files
        store = []
        for filenum, filename in enumerate(glob(filepath)):
            # logger.info(f"\tLoading file number {filenum}")
            tmp = pd.read_csv(filename, header=0)
            store.append(tmp)
        keep_index = index is None
        df = pd.concat(store, axis=0, ignore_index=keep_index)
    # Load a single file
    else:
        df = pd.read_csv(filepath, header=0)
    # Set index of our data
    if index is not None:
        df = df.set_index(index)
    self.df = df
    # def __init__(self, filepath:str) -> None:
    #     """
    #     Loads a CSV file path into a Dataframe

    #     Args:
    #         filepath (str): file path to the CSV file
    #     """
    #     pass

    # - Fill in the head() method so it prints the head of the DataFrame.
    def head(self) -> None:
        """
        prints the head of the dataframe to console
        """
        print(self.df.head())

    def add_index(self, col_list, index_name="index") -> None:
        """
        Creates an index by concatenating columns values
        """
        df = self.df
        # Concatenates columns, calls it col_list and turns it into the primary key aka index
        df[index_name] = df[col_list].apply(lambda row: "-".join(row.values.astype(str)), axis = 1)
        df.set_index(index_name, inplace = True)
        # replaces whatever the index used to be with our shiny, new, smushed up index
        self.df = df

    def sort(self, column_name:str) -> None:
        df = self.df
        # sorts the dataframe by a later specified column
        df.sort_values(column_name)
        self.df = df

    def load_to_db(self, db_engine, db_table_name:str) -> None:
        """
        Loads the dataframe into a database table.

        Args:
            db_engine (SqlAlchemy Engine): SqlAlchemy engine (or connection) to use to insert into database
            db_table_name (str): name of database table to insert to
        """
        self.df.to_sql(name=db_table_name, con=self.engine, if_exists='append', chunksize=2000)


"""Outside the DataLoader class, fill in the db_engine() function to create a SQLAlchemy database engine. For this project, the values needed to configure it can be passed in as arguments, rather than using a config file.
"""
def db_engine(db_host:str, db_user:str, db_pass:str, db_name:str="spotify") -> sa.engine.Engine:
    
    """Using SqlAlchemy, create a database engine and return it

    Args:
        db_host (str): datbase host and port settings
        db_user (str): database user
        db_pass (str): database password
        db_name (str): database name, defaults to "spotify"

    Returns:
        sa.engine.Engine: sqlalchemy engine
    """
    #create enginge using sqlalchmey
    engine = create_engine(f'mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}', future = True)
    metadata = MetaData(bind=engine)
    conn = engine.connect()
    return engine


def db_create_tables(db_engine, drop_first:bool = False) -> None:
    """
    Using SqlAlchemy Metadata class create two spotify tables (including their schema columns and types)
    for **artists** and **albums**.


    Args:
        db_engine (SqlAlchemy Engine): SqlAlchemy engine to bind the metadata to.
        drop_first (bool): Drop the tables before creating them again first. Default to False
    """
    meta = sa.MetaData(bind=db_engine)



class DataTable():
        """Create an object that can update the data in our mariaDB tables"""

    def db_engine(db_host:str, db_user:str, db_pass:str, db_name:str="spotify") -> sa.engine.Engine:
        super().__init__()
        """Loads the dataframe into a database table."""
            # create an engine and connection to our docker mariadb
        db_host = config['db_host'] if db_host is None else db_host
        db_user = config['db_user'] if db_user is None else db_user
        db_pass = config['db_pass'] if db_pass is None else db_pass
        db_name = config['db_name'] if db_name is None else db_name

        engine = create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}", future=True)
        metadata = MetaData(bind=engine)
        conn = engine.connect()
        self.engine = engine
        self.metadata = metadata
        self.conn = conn

    def open(self):
        conn = self.engine.connect()
        self.conn = conn


    def close(self):
        conn = self.conn
        conn.close()

        self.df.to_sql(name=db_table_name, con=self.engine, if_exists='append', chunksize=2000)



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