import pandas as pd
import sqlalchemy
import sqlite3
def read_data() -> pd.DataFrame:
    """Read in the data from the SQL database"""
    engine = sqlalchemy.create_engine(
        'sqlite:////Users/rura/Desktop/sql/fda_test.db')

    # Have to account for SQLAlchemy v2
    with engine.connect() as conn:
        df = pd.read_sql(sqlalchemy.text(
            'SELECT id, companyname FROM company_name'), conn)
    return df
def append_to_database(df: pd.DataFrame):
    ''' This will add the dataframe into SQL
        :param df(pd.Dataframe)
    '''
    engine = sqlalchemy.create_engine('sqlite:////Users/rura/Desktop/sql/fda_test.db')
    #pushing it into sql

    with engine.connect() as conn:
        df2 = df.to_sql('fda', engine, index=False, if_exists='replace')
def makedatabase():
    company_name = '''
        CREATE TABLE IF NOT EXISTS company_name(
            id INTEGER PRIMARY KEY,
            companyname VARCHAR(250) NOT NULL
        );'''
    drug_name = '''
        CREATE TABLE IF NOT EXISTS drug_name(
            id INTEGER PRIMARY KEY, 
            drugname VARCHAR(250) NOT NULL, 
            company_id INTEGER REFERENCES company_name(id) NOT NULL 
        );'''
    raw_data = '''
        CREATE TABLE IF NOT EXISTS raw_data(
            applicant VARCHAR(250) NOT NULL,
            companyname VARCHAR(250) NOT NULL, 
            drugname VARCHAR(250) NOT NULL
        );
    '''
    matched_company = '''
        CREATE TABLE IF NOT EXISTS matched_company(
            parent_company_id INTEGER NOT NULL,
            child_company_id INTEGER NOT NULL,
            UNIQUE(parent_company_id, child_company_id)
        );
    '''

    sqliteConnection = sqlite3.connect('fda_test.db')
    cursor = sqliteConnection.cursor()
    print("Database created and Successfully Connected to SQLite")

    cursor.execute(drug_name)
    cursor.execute(company_name)
    cursor.execute(raw_data)
    cursor.execute(matched_company)

    cursor.close()
def table_insert():
    company_name_sql = '''
        INSERT INTO company_name(applicant, id)
        SELECT DISTINCT applicant, id
        FROM raw_data
        ORDER BY applicant'''
    drug_name_sql = '''
        INSERT INTO drug_name(drugname)
        SELECT  proprietary_name
        FROM fda'''
    raw_data_sql = '''
        INSERT INTO raw_data( companyname, propername, drugname)
        SELECT applicant, proper_name, proprietary_name
        FROM fda
        '''
    sqliteConnection = sqlite3.connect('fda_test.db')
    cursor = sqliteConnection.cursor()
    print("Database created and Successfully Connected to SQLite")

    cursor.execute(drug_name_sql)
    cursor.execute(company_name_sql)
    cursor.execute(raw_data_sql)

    cursor.close()

def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    '''
    This will deduplicate company names
    param
        df (pd.DataFrame) : dataframe of unique company names
    :return:
        pd.DataFrame: bridge table
    '''
    bridge_table = []
    for i, row in df.iterrows():
        for _, row2 in df.iloc[i+1:].iterrows():
            if row['companyname'] in row2['companyname']:
                bridge_table.append([row['id'], row2['id']])
    return pd.DataFrame(bridge_table, columns= ['parent_ID','child_ID'])

def send_to_sql(df:pd.DataFrame):
    ''' This add the dataframe into sql
    param
     df(pd.DataFrame): Bridge table
    '''
    #the four slashes are for sqlite
    engine = sqlalchemy.create_engine('sqlite:////Users/rura/Desktop/sql/fda_test.db')
    #pushing it into sql
    with engine.connect() as conn:
        df2 = df.to_sql('bridge_table', engine, index=False, if_exists='replace')
    return df2

def dedup_dedup(co_df: pd.DataFrame, bridge_df: pd.DataFrame) -> pd.DataFrame:
    comb = bridge_df.merge(co_df[['id', 'companyname']].rename(
        columns={'companyname': 'parent_name'}), how='left', left_on='parent_ID', right_on='id')
    comb = comb.merge(co_df[['id', 'companyname']].rename(
        columns={'companyname': 'child_name'}), how='left', left_on='child_ID', right_on='id')
    # comb['keep'] = comb.groupby('parent_ID').apply(sort_apply, axis=1)
    return comb
if __name__ == "__main__":
    df = pd.read_csv('/Users/rura/Downloads/fda_purple_orange_books.csv')
    append_to_database(df)
    # companyname_table = read_data()
    # bridge = deduplicate(companyname_table)
    # bridge_tab = dedup_dedup(companyname_table, bridge)
    # send_to_sql(bridge_tab)














# #the four slashes are for sqlite
# engine = sqlalchemy.create_engine('sqlite:////Users/rura/Desktop/sql/fda_test.db')
# #connects to sql databases
#
# df = pd.read_csv('/Users/rura/Downloads/fda_purple_orange_books.csv')
# df.to_sql('fda', engine, index = False, if_exists='append')
# # pushing it into sql
#
# #reading it from sql
# # have to account for sqlalchemy v2
# with engine.connect() as conn:
#     df2 = pd.read_sql(sqlalchemy.text('SELECT *FROM fda'), conn)
#     # read_sql  = to_squl
#     # this is ensuring that there are no hidden command
# print(df2)
