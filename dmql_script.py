# Created By: Param Patel(UBID: paramnar, UB_Number:50560141)
# This script is used to convert the downloaded csv files into database files.
        # +++++++++         +++++++++
        # +  CSV  +  ====>  +  .DB  +
        # +++++++++         +++++++++   
#==================================================================================================================
# The dataset used by our team is from Kaggle regarding Formula1. Below is the link:
# https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020
#==================================================================================================================
# DISCLAIMER: PLEASE MAKE SURE TO SAVE THE SCRIPT IN THE SAME LOCATION AS THE CSV FILES!
#==================================================================================================================
# The purpose of creating this script is to simplify our task by dynamically converting all the csv files into 
# Database files (.db files) and at the same time, loading them in the our postgres database.
# This saves us time and trouble of creating the tables and thus achieving better efficiency.
#==================================================================================================================
#Importing Libraries
from sqlalchemy import create_engine, Table, Column, Integer,Float, String, MetaData
import psycopg2
import pandas as pd
import os
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
#==================================================================================================================
path = "/home/uraharakisuke/Downloads/archive/csv/"
csv_list = [var_1 for var_1 in os.listdir(path) if var_1.endswith('.csv')]
#==================================================================================================================
#ENGINE, BASE, SESSION Creation.
engine = create_engine('postgresql://uraharakisuke:dazai@localhost:5432/localhost') 
# If you want to run in you computer then you need to change the 
# username,PORT(if you're using a different one) and the name of the database.
#Below is the sample database for how to run the engine script:
#==================================================================================================================
# engine=create_engine('postgresql:username:password@localhost:5432//database_name)
# Change the username,password and the database name to your respective username,password and database_name
# By default, the PORT value for Postgresql is 5432. However, if you have a different port then change it as well.
#==================================================================================================================
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()
#==================================================================================================================
for var_csv in csv_list:
    TableName = os.path.splitext(var_csv)[0]
    DataFrame = pd.read_csv(os.path.join(path, var_csv))

    class DynamicTable(Base):
        __tablename__ = TableName
        id = Column(Integer, primary_key=True)
        for column in DataFrame.columns:
            exec(f'{column} = Column(String)')
    Base.metadata.create_all(engine)
    DataFrame.to_sql(TableName, con=engine, if_exists='append', index=False)
#==================================================================================================================
session.commit()
session.close()
#==================================================================================================================
