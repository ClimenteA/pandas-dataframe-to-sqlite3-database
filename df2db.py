
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


# In[68]:


class Df2db:
    
    def __init__(self, dbname, root_path):
        self.dbname = dbname
        self.root_path = root_path    
    
    def connect_db(self):
        #Connect to a db and if it not exists creates one with the name given
        connection = sqlite3.connect(self.dbname)
        cursor = connection.cursor()
        return connection, cursor
    
    def close(self):
        connection, cursor = Df2db(self.dbname, self.root_path).connect_db()
        connection.commit()
        connection.close()
    
    def norm_punctmarcks(self, astring):
        #Removing punctuation marks from the string, necessary for making compatible table names
        punctuation_marks = list(str(string.punctuation).replace('_', ''))+[' ']
        try:
            for char in punctuation_marks:
                astring = astring.replace(char, '_')
        except:
            pass
        return astring
    
    def rename_duplicate_dfcols(self, df):
        #Rename DF columns if found duplicates (credit to SO"Lamakaha")
        try:
            cols=pd.Series(df.columns)
            for dup in df.columns.get_duplicates(): 
                cols[df.columns.get_loc(dup)]=[dup+'.'+str(d_idx) if d_idx!=0 else dup for d_idx in range(df.columns.get_loc(dup).sum())]
                df.columns=cols
        except Exception as e:
            print("Got ", e)
            pass
        return df
    
    
    def df_tosql(self, path_to_xlname, dfname=''):
        #Get thru all sheets and if it has data save it to db
        connection, cursor = Df2db(self.dbname, self.root_path).connect_db()
        extension = path_to_xlname[-4:]
        try:
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
                    #Rename duplicate columns from df
                    df_sht = Df2db(self.dbname, self.root_path).rename_duplicate_dfcols(df_sht)
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
                    tablename_insql = str('EXCEL_' + xlname + '_SHEET_Sheet1')
                    tablename_insql = Df2db(self.dbname, self.root_path).norm_punctmarcks(tablename_insql)
                    xl.rename(columns=lambda x: x.strip().replace(' ', '_'), inplace=True)
                    #Rename duplicate columns from df
                    xl = Df2db(self.dbname, self.root_path).rename_duplicate_dfcols(xl)
                    xl.to_sql(tablename_insql, connection, if_exists="replace", index=False)
                    connection.commit()
                    print('{} ok'.format(tablename_insql))
            else:
                print('{} NOT SAVED!'.format(path_to_xlname))
                pass
        except:
            try:
                if dfname == '': 
                    print("Insert the df and the dfname!!")
                    sys.exit()
                df = path_to_xlname
                tablename_insql = dfname
                tablename_insql = str('EXCEL_' + tablename_insql + '_xlsx_SHEET_Sheet1')
                tablename_insql = Df2db(self.dbname, self.root_path).norm_punctmarcks(tablename_insql)
                df.rename(columns=lambda x: x.strip().replace(' ', '_'), inplace=True)
                #Rename duplicate columns from df
                df = Df2db(self.dbname, self.root_path).rename_duplicate_dfcols(df)
                df.to_sql(tablename_insql, connection, if_exists="replace", index=False)
                connection.commit()
                print('{} ok'.format(tablename_insql))
            except Exception as e:
                print('NOT SAVED! \n{}'.format(e))
                pass
            
            
            
            
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
            print("Error: {},\n{}".format(e, df))
            pass
        
    


# In[ ]:


"""
#Import class Df2db from df2db module
from df2db import Df2db

#Instantiate with the database name and the path where you got excel files
todf = Df2db('dbname.db', r'D:\alot_of_xlfiles')

#Call dfs_tosql function to search in the path you give for excel files 
#and save them to the database with the name given
todf.dfs_tosql()

#Show tables from the database
todf.show_db_tables()

#Get a table from the database as a dataframe object
#in order to use it in pandas for manipulation
todf.getdf_fromdb("EXCEL_xl4_xlsx_SHEET_Sheet1")

#Delete a table from the database
todf.drop_tablefrom_db("EXCEL_xl4_xlsx_SHEET_Sheet1")

#Show tables from db, now you see one it's gone
todf.show_db_tables()

#Close the connection when you are done.
todf.close()

#Instantiate with the database name and the path where you got excel files
todf = Df2db('dbname.db', r'D:\alot_of_xlfiles')

#Make a dataframe
import numpy as np
import pandas as pd
dates = pd.date_range('20130101', periods=6)
df = pd.DataFrame(np.random.randn(6,4), index=dates, columns=list('ABCD'))

#Save it to sql, you must give the df and a name for the df
todf.df_tosql(df, 'test')

#Show current tables from db 
todf.show_db_tables()
#Close when you are done
todf.close()

#Each table saved in database will have this form
tablname = "EXCEL_test_xlsx_SHEET_Sheet1"
tablname.split('_')


'EXCEL' - all tables in db will start with this prefix
'test' - the name if the excel or df 
'xlsx' - the extension of the file
'SHEET' - the next folowing this will be the Sheet name of the excel
That's because it looks in the workbook in all sheets for tables and saves the sheet name also
'Sheet1' - The sheet where the table was found

If the excel or df has punctuation marks or spaces those will be replaced with underscore "_"
This replace is done in order to be compatible with sqlite3 database
Also, if the column names contains spaces those will be replaced with "_"

"""

