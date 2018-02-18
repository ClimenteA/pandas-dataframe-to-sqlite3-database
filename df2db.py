
# coding: utf-8

# In[1]:


import sqlite3
import pandas as pd
import os, sys, re, string
import itertools


# In[ ]:


"""
This module can be used to add from a parent folder all the excel files to a sqlite3 database.
The class requires the database name and the path where you have the excel files.

Warning: If it founds an .csv file it will save to database only the first sheet!
Requires pandas module to be installed.

"""


# In[2]:


class Df2db:
    
    def __init__(self, dbname, root_path):
        self.dbname = dbname
        self.root_path = root_path
    
    def connect_db(self):
        #Connect to a db and if it not exists creates one with the name given
        connection = sqlite3.connect(self.dbname)
        cursor = connection.cursor()
        return connection, cursor
        
    def norm_punctmarcks(self, astring):
        #Removing punctuation marks from the string, necessary for making compatible table names
        punctuation_marks = list(str(string.punctuation).replace('_', ''))+[' ']
        try:
            for char in punctuation_marks:
                astring = astring.replace(char, '_')
        except:
            pass
        return astring
    
    def df_tosql(self, path_to_xlname):
        #Get thru all sheets and if it has data save it to db
        connection, cursor = Df2db(self.dbname, self.root_path).connect_db()
        extension = path_to_xlname[-4:]
        if re.search('xlsx', extension) or re.search('xls', extension) or re.search('xlsm', extension):
            #If excel file then
            xl = pd.ExcelFile(path_to_xlname)
            for sht in xl.sheet_names:
                df_sht = xl.parse(sht)
            if df_sht.shape == (0,0): 
                pass
            else:
                xlname = path_to_xlname.strip().split('\\')[-1]
                tablename_insql = str('EXCEL_' + xlname + '_SHEET_' + sht)
                tablename_insql = Df2db(self.dbname, self.root_path).norm_punctmarcks(tablename_insql)
                #Replacing spaces with underscores to be compatible to sql tables
                df_sht.rename(columns=lambda x: x.strip().replace(' ', '_'), inplace=True)
                #Saving df to sqlite3 db
                df_sht.to_sql(tablename_insql, connection, if_exists="replace", index=False)
                connection.commit()
                print('{} ok'.format(tablename_insql))
        elif re.search('.csv', extension):
            #If csv file then
            xl = pd.read_csv(path_to_xlname)   
            if xl.shape == (0,0): 
                pass
            else:
                xlname = path_to_xlname.strip().split('\\')[-1]
                tablename_insql = str('EXCEL_' + xlname + '_SHEET_' + 'Sheet1')
                tablename_insql = Df2db(self.dbname, self.root_path).norm_punctmarcks(tablename_insql)
                xl.rename(columns=lambda x: x.strip().replace(' ', '_'), inplace=True)
                xl.to_sql(tablename_insql, connection, if_exists="replace", index=False)
                connection.commit()
                print('{} ok'.format(tablename_insql))
        else:
            print('{} NOT SAVED!'.format(path_to_xlname))
            
    def getdf_fromdb(self, table_name):
        #gets the table from the db
        #the table name must have this format excel_xlname_sheet_sheetname
        connection, cursor = Df2db(self.dbname, self.root_path).connect_db()
        query = "SELECT * FROM {}".format(table_name)
        df = pd.read_sql_query(query, connection)
        return df

    def show_db_tables(self):
        #Shows all tables names from the db
        connection, cursor = Df2db(self.dbname, self.root_path).connect_db()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        all_tb = cursor.fetchall()
        #print(all_tb)
        all_tb = [x[0] for x in all_tb]
        column_name = '{}_TABLES'.format(self.dbname)
        df = pd.DataFrame({column_name : all_tb})
        try:
            df.to_csv(column_name+'.csv', index=False)
        except Exception as e:
            input('CSV already openned please close it..\n {}'.format(e))
            sys.exit()
        print('{} tables in {}\n'.format(len(all_tb), self.dbname))
        for tb in all_tb:
            print(tb)
    
    def drop_tablefrom_db(self, table_name):
        #Using the cursor created in connect_db func executes statement
        connection = sqlite3.connect(self.dbname)
        cursor = connection.cursor()
        query = 'DROP TABLE {};'.format(table_name)
        try:
            cursor.execute(query)
        except Exception as e:
            print("Table does not exist!\nPress Enter to exit...")
            input(e)
            sys.exit()
    
    
    def getfilespath_from(self):
        #Walk thru a start path and return a list of paths to files
        allfiles = []
        for root, dirs, files in os.walk(self.root_path):
            for file in files:
                path_tofile = root + '\\' + file
                allfiles.append(path_tofile)
        return allfiles

    
    def get_dfpaths(self):
        #get a list of paths to excel files (.xlsx, .xls, .xlsm, .csv)
        paths_tofiles = Df2db(self.dbname, self.root_path).getfilespath_from()
        filtered_extensions = []
        for path in paths_tofiles:
            p = path.split('\\')
            if re.search('.xls', p[-1]) or re.search('.csv', p[-1]):
                filtered_extensions.append(path)
        return filtered_extensions
    
    def dfs_tosql(self):
        #Run thru alldfs and save them to db
        path_to_dfs = Df2db(self.dbname, self.root_path).get_dfpaths()
        try:
            for df in path_to_dfs:
                Df2db(self.dbname, self.root_path).df_tosql(df)
        except Exception as e:
            print("nok", e)
            pass

