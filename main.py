import csv, sqlite3
import pandas as pd
import numpy as np

# 1) Finding any missing data and removing row associated with data

def missing_data(data_path):

    df = data_path.dropna(how='any').shape

    return df


# 2) Checking for and getting rid of duplicates

def drop_dupplicates(data_path):

    df = data_path.drop_duplicates(keep='first').shape

    return df


# 3) Getting rid of product name as required by task

def column_removal(data_path, column_name):

    df = data_path.drop(column_name, 1)

    return df


# 4) Swap first two columns as required by task

def swapping_columns(data_path):

    df = pd.DataFrame(data_path)

    columnsTitles = ['review', 'title', 'iso', 'score', 'date', 'app_bought', 'money_spent']

    df = df.reindex(columns=columnsTitles)

    return df


# 5) Creating money_spent and apps_bought buckets/bins

def creating_buckets(data_path, column_name):

    num_buckets = 3;

    group_names = ['low', 'medium', 'high']

    df = pd.DataFrame(data_path)

    max = df[column_name].max()

    min = df[column_name].min()

    buckets = [min, (max*(1/num_buckets)), (max*(2/num_buckets)), max]

    df[column_name+'_bucket'] = pd.cut(df[column_name], buckets, labels=group_names)


# 6) Creating new CSV file and dumping clean data

def new_CSV(data_path, csv_file_name):

    df = pd.DataFrame(data_path, columns=['review', 'title', 'iso', 'score', 'date', 'app_bought', 'money_spent', 'app_bought_bucket', 'money_spent_bucket'])

    df.to_csv(csv_file_name, index=False)

    return df


# 7) Putting data into SQLite3 database..

def df_to_sql(new_data_path, db_name, table_name):

    # Stripping whitespace from headers

    df = pd.DataFrame(new_data_path)

    df.columns = df.columns.str.strip()

    con = sqlite3.connect(db_name)

    df.to_sql(table_name, con)

    con.close()

    return df


# 8) Execution of functions above

reviews = pd.read_csv("reddit_exercise_data.csv", encoding='utf8')

missing_data(reviews)

drop_dupplicates(reviews)

column_name = 'product_name'

column_removal(reviews, column_name)

swapping_columns(reviews)

# buckets for app_bought

creating_buckets(reviews, 'app_bought')

# buckets for money_spent

creating_buckets(reviews, 'money_spent')

# new CSV data

new_CSV(reviews, 'clean_data.csv')

# Creating new database with new clean data

clean_data =pd.read_csv("clean_data.csv", encoding='utf8')

print(clean_data)

db_name = 'exercise_database.db'

table_name = 'reviews'
