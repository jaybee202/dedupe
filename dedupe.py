import numpy as np
import polars as pl
import time
## Import data and format column names to be lower and use _ instead of space
dfd = pl.read_csv('superstore_with_5_dupes.csv')

new_cols = {}
for c in dfd.columns:
    new_cols[c] = c.lower().replace(' ', '_')

dfd = dfd.rename(new_cols)
# df.head(2)
## Import data and format column names to be lower and use _ instead of space
df = pl.read_csv('superstore.csv')

new_cols = {}
for c in df.columns:
    new_cols[c] = c.lower().replace(' ', '_')

df = df.rename(new_cols)
# df.head(2)
## Create class object
class Dupe:
    def __init__(self, data, key = None, suggest_key = False):
        self.data = data
        self.key = key
        self.suggest_key = suggest_key
        self.dupe_data = None

    
    ## Function to set the Dupe class item key, or column that should be unique
    def set_key(self, key):
        self.key = key

    
    ## Function to get information about the data and key in the Dupe class object
    def get_info(self):
        message = ''
        if self.key == None:
            message += 'No key is currently assigned\n'
        else:
            message += f'Key value: {self.key}\n'

        s = self.data.shape
        message += f'Your data includes {s[0]} rows and {s[1]} columns'
        print(message)

    
    ## Function to show the head of the dataframe in the Dupe class object, defaults to five rows
    def show_data(self, rows = 5):
        return self.data.head(rows)

    
    ## Function that suggests best candidates for unique ids in dataframe
    def get_key_suggestion(self, n_suggestions = 3):
        ## Get unique values for each column        
        cols = {'column': [], 'unique': []}
        uni = 0
        
        for c in self.data.columns:
            cols['column'].append(c)
            cols['unique'].append(self.data[c].unique().shape[0])
        
        ## Check if any columns are completely unique
        suggested_keys = []
        df_rows = self.data.shape[0]
        sug_key = pl.DataFrame(cols)
        
        ## Return a list of completely unique columns
        unique_cols = sug_key.filter(pl.col('unique') == df_rows).shape[0]
        if unique_cols > 0:
            suggested_keys = sug_key.filter(pl.col('unique') == df_rows)['column'].to_list()
            if len(suggested_keys) == 1:
                message = 'Your data includes one unique id\n'
            else:
                message = 'Your data includes unique ids\n'
        
        ## Otherwise, return the top N options based on highest unique values
        else:
            suggested_keys = sug_key.sort('unique', descending = True).head(n_suggestions)['column'].to_list()
            message = f'The top {n_suggestions} suggestions for a unique id include:\n'

        key_messages = []
        key_message = ''
        pct_uni = 0.0
        bullet = 1
        
        for key in suggested_keys:
            unique_count = sug_key.filter(pl.col('column') == key).select('unique').item()
            pct_uni = f'{unique_count / df_rows * 100:.2f}%'
            
            key_message = f"\t{bullet}. {key}: {pct_uni} unique ({unique_count} of {df_rows} unique)"
            key_messages.append(key_message)
            bullet += 1

        key_messages = '\n'.join(key_messages)
        print(message + key_messages)

    ## Function that identifies the columns responsible for duplicating a specified column
    def find_dupe_cols(self, ignore_cols = []):
        print('Setting everything up')

        dfd = self.data
        key = self.key
        unique_keys = self.data[key].unique().to_list()
        uk_len = len(unique_keys)
        uk_rows, col_rows = 0, 0
        uk_df = pl.DataFrame()
        key_header = f'{key}_value'
        dupe_dict = {key_header: [], 'dupe_col': [], 'dupe_count': [], 'dupe_vals': []}

        print(f'Starting to process {uk_len} rows of data')
        
        for uk in unique_keys:
            uk_df = self.data.select(key).filter(pl.col(key) == uk)
            uk_rows = uk_df.shape[0]

            if uk_rows > 1:
                for col in [col for col in dfd.columns if col != key]:
                    uk_df = dfd.filter(pl.col(key) == uk).select([key, col]).unique()
                    col_rows = uk_df.shape[0]
                    if col_rows > 1:
                        diff_vals = uk_df[col].to_list()
                        dupe_dict[key_header].append(uk)
                        dupe_dict['dupe_col'].append(col)
                        dupe_dict['dupe_count'].append(col_rows)
                        dupe_dict['dupe_vals'].append(diff_vals)
            
        self.dupe_data = pl.DataFrame(dupe_dict)
        print('Duplicates discovered')
        return self.dupe_data
