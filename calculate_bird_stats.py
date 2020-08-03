#!/usr/bin/env python
from sqlalchemy import create_engine
import pandas as pd


def write_to_db(data, table):
    """Writes dataframe to DB in case the table doesn't have any records"""
    if pd.read_sql_query("select * from {};".format(table), engine).empty:
        data.to_sql(table, engine, if_exists='append', index=False)
    else:
        print("Table '{}' already has data".format(table))


engine = create_engine('postgresql://ornithologist:ornithologist@localhost:5432/birds_db')
birdsDf = pd.read_sql_query("select * from birds;", engine)

# Calculate amount of birds of the specific color
birdColorsDf = birdsDf.groupby(by=["color"])["name"].count().reset_index(name='count')
write_to_db(birdColorsDf, 'bird_colors_info')

# Calculate Mean, Median and Mode of the Length and wingspan_length
birdStatDf = pd.DataFrame({'body_length_mean': [birdsDf['body_length'].mean()],
                           'body_length_median': [birdsDf['body_length'].median()],
                           'body_length_mode': [birdsDf['body_length'].mode().values.tolist()],
                           'wingspan_mean': [birdsDf['wingspan'].mean()],
                           'wingspan_median': [birdsDf['wingspan'].median()],
                           'wingspan_mode': [birdsDf['wingspan'].mode().values.tolist()]})
write_to_db(birdStatDf, 'birds_stat')
