import sqlite3
import pandas as pd
import os, sys, re, string
import itertools

# """
# This module can be used to add from a parent folder all the excel files to a sqlite3 database.
# The class requires the database name and the path where you have the excel files.

# Warning: If it founds an .csv file it will save to database only the first sheet!
# Requires pandas module to be installed.

# """

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
    
    def norm_pctmarks(self, astring):
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
    
    
    def save_tosql(self,connection, df_sht, sht, path_to_xlname):
        
        xlname = path_to_xlname.strip().split('\\')[-1]
        try:
            tablename_insql = str(xlname + '_ONSHEET_' + sht)
        except:
            tablename_insql = str(xlname)
        
        #Remove punctuation marks from table name
        tablename_insql = Df2db(self.dbname, self.root_path).norm_pctmarks(tablename_insql)
        
        #Dealing with txt files   
        ext = path_to_xlname.split('.')[-1]
        if ext == 'txt':
            print(path_to_xlname)
            txt = open(path_to_xlname).read().splitlines()
            df = pd.Series(txt)
            df = pd.DataFrame({tablename_insql: df})
            #Saving df to sqlite3 db
            df.to_sql(tablename_insql, connection, if_exists="replace", index=False)
            connection.commit()
            print('{} ok'.format(tablename_insql))
            
        elif ext == 'csv':
            
            df = pd.read_csv(path_to_xlname)
    
            #Replacing incompatible with sqlite3 chars with underscores 
            cols_names = df.columns.values
            new_cols_names = []
            for col in cols_names:
                newcol_name = Df2db(self.dbname, self.root_path).norm_pctmarks(col)
                new_cols_names.append(newcol_name)
            new_cols_names = [str(n).replace('\n', '_') for n in new_cols_names]
            new_cols_names = [str(n).replace('___', '_') for n in new_cols_names]
            new_cols_names = [str(n).replace('__', '_') for n in new_cols_names]
            df.columns = new_cols_names
            
            #Rename duplicate columns from df
            df = Df2db(self.dbname, self.root_path).rename_duplicate_dfcols(df)
        
            #Saving df to sqlite3 db
            df.to_sql(tablename_insql, connection, if_exists="replace", index=False)
            connection.commit()
            print('{} ok'.format(tablename_insql))
        
        else:
            #Replacing incompatible with sqlite3 chars with underscores 
            cols_names = df_sht.columns.values
            new_cols_names = []
            for col in cols_names:
                newcol_name = Df2db(self.dbname, self.root_path).norm_pctmarks(col)
                new_cols_names.append(newcol_name)
            new_cols_names = [str(n).replace('\n', '_') for n in new_cols_names]
            new_cols_names = [str(n).replace('___', '_') for n in new_cols_names]
            new_cols_names = [str(n).replace('__', '_') for n in new_cols_names]
            df_sht.columns = new_cols_names
            
            #Rename duplicate columns from df
            df_sht = Df2db(self.dbname, self.root_path).rename_duplicate_dfcols(df_sht)
        
            #Saving df to sqlite3 db
            df_sht.to_sql(tablename_insql, connection, if_exists="replace", index=False)
            connection.commit()
            print('{} ok'.format(tablename_insql))
        
        
    
    
    def xl2sql(self, path_to_xlname):
        connection, cursor = Df2db(self.dbname, self.root_path).connect_db()
        df = pd.ExcelFile(path_to_xlname)
        for sht in df.sheet_names:
            df_sht = df.parse(sht)
            if df_sht.shape == (0,0): 
                pass
            else:
                print("has")
                Df2db(self.dbname, self.root_path).save_tosql(connection, df_sht,sht,path_to_xlname)
                           
    def df2sql(self, df, dfname):
        connection, cursor = Df2db(self.dbname, self.root_path).connect_db()
        #print('conn si cursor ok\n', type(df), dfname)
        #Saving df to sqlite3 db
        df.to_sql(dfname, connection, if_exists="replace", index=False)
        connection.commit()
        print('{} ok'.format(dfname))
        #Df2db(self.dbname, self.root_path).save_tosql(connection, df_sht,sht,path_to_xlname)
        
    def csv2sql(self, path_to_xlname, df='', sht='Sheet1'):
        connection, cursor = Df2db(self.dbname, self.root_path).connect_db()
        Df2db(self.dbname, self.root_path).save_tosql(connection, df, sht, path_to_xlname)
        
    def txt2sql(self,path_to_txt, df='', sht=''):
        connection, cursor = Df2db(self.dbname, self.root_path).connect_db()
        Df2db(self.dbname, self.root_path).save_tosql(connection, df, sht, path_to_txt)
        
    
    def df_tosql(self, path_to_xlname, dfname=''):
        #Get thru all sheets and if it has data save it to db
        try:
            filename = path_to_xlname.split('\\')[-1]
        except:
            pass
        
        #input('trecut de trypass')
    
        if str(type(path_to_xlname)) == "<class 'pandas.core.frame.DataFrame'>":

            #input('recunoscut ca df')
            
            Df2db(self.dbname, self.root_path).df2sql(path_to_xlname, dfname)
        
        elif re.search('.xls', filename):
            print('.xls')
            Df2db(self.dbname, self.root_path).xl2sql(path_to_xlname)
                
            
        elif re.search('.csv', filename):
            print('.csv')
            Df2db(self.dbname, self.root_path).csv2sql(path_to_xlname)
            
        elif re.search('.txt', filename):
            print('.txt')
            Df2db(self.dbname, self.root_path).txt2sql(path_to_xlname)
    
    
            
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
        all_tb = [x[0] for x in all_tb] # from [(table1, ) etc] makes [table1, etc]
        column_name = '{}_TABLES'.format(self.dbname)
        df = pd.DataFrame({column_name : all_tb})
        try:
            df.to_csv(column_name+'.csv', index=False)
        except Exception as e:
            input('CSV already openned please close it..\n {}'.format(e))
            sys.exit()
        print('{} tables in {}\n'.format(len(all_tb), self.dbname))
        return all_tb
        
        
        
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
        #get a list of paths to excel files (.xlsx, .xls, .xlsm, .csv, .txt)
        paths_tofiles = Df2db(self.dbname, self.root_path).getfilespath_from()
        filtered_extensions = []
        files_with_issues = []
        for path in paths_tofiles:
            file = path.split('\\')[-1]
            tempfile = "".join(list(file)[:2])

            if tempfile == "~$": # if file open then append to list the path
                print('This file {} is open'.format(path))
                files_with_issues.append(path)
                
            elif re.search('.xls', file) or re.search('.csv', file) or re.search('.txt',  file):
                filtered_extensions.append(path)   
        
        #Create a txt file with files that where open
        if len(files_with_issues) == 0:
            pass
        else:
            files_with_errors = '\n'.join(files_with_issues) 
            error_file = open("Files that were open.txt", 'w')
            error_file.write(files_with_errors)
            error_file.close()
        
        return filtered_extensions
    
    
    def dfs_tosql(self):
        #Run thru alldfs and save them to db
        path_to_dfs = Df2db(self.dbname, self.root_path).get_dfpaths()
        print('\nThere are ',len(path_to_dfs), ' files to be added..\n\n')
        for df in path_to_dfs:
            #print(df)
            Df2db(self.dbname, self.root_path).df_tosql(df)
        #tab = Df2db(self.dbname, self.root_path).show_db_tables()
        
# #### Instantiating the class
# todf = Df2db('mydata.db', r'E:\sqlite3')      


# # In[4]:


# orase = pd.read_csv('orase.csv')
# jud_loc = orase[['JUDET', 'LOCALITATE']]


# # In[22]:


# todf.df_tosql(jud_loc, 'judloc')


# # In[35]:


# todf.show_db_tables()


# # In[36]:


# todf.getdf_fromdb("judloc")


# # In[26]:


# todf.drop_tablefrom_db("orase_csv_ONSHEET_Sheet1")


# In[27]:


# todf.show_db_tables()


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

