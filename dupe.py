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

    def set_key(self, key):
        self.key = key

    ## Function to get or set key
    def get_info(self):
        message = ''
        if self.key == None:
            message += 'No key is currently assigned\n'
        else:
            message += f'Key value: {self.key}\n'

        s = self.data.shape
        message += f'Your data includes {s[0]} rows and {s[1]} columns'
        print(message)

    def show_data(self, rows = 5):
        return self.data.head(rows)

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
        if sug_key.filter(pl.col('unique') > df_rows + 1).shape[0]:
            suggested_keys = sug_key.filter(pl.col('unique') > df_rows + 1)['column'].to_list()
        
        ## Otherwise, return the top N options based on highest unique values
        else:
            suggested_keys = sug_key.sort('unique', descending = True).head(n_suggestions)['column'].to_list()
        
        print(f'Your data includes {df_rows} rows\nThe top {n_suggestions} suggestions for a unique id include:')
        
        ([print(f"   - {c} includes {sug_key.filter(pl.col('column') == c).select('unique').item()} unique values") 
          for c in suggested_keys])

    def find_dupe_cols(self, ignore_cols = []):
        print('Setting everything up')
        
        key = self.key
        unique_keys = self.data[key].unique().to_list()
        uk_len = len(unique_keys)
        uk_rows, col_rows = 0, 0
        dupe_dict = {f'{key}_value': [], 'dupe_col': [], 'different_vals': []}

        print(f'Starting to process {uk_len} rows of data')
        count = 1
        
        for uk in unique_keys:
            uk_rows = (self.data.with_columns(pl.col(key).cast(pl.String), pl.lit(1).alias('cnt'))
             .select([key, 'cnt'])
             .filter(pl.col(key).cast(pl.String) == str(uk))
             .group_by(key).sum()['cnt'].item()
            )
        
            if uk_rows > 1:
                for c in [c for c in self.data.columns if c != key]:
                    col_rows = self.data.filter(pl.col(key) == uk).select([key, c]).unique().shape[0]
                    if col_rows > 1:
                        dupe_dict[f'{key}_value'].append(uk)
                        dupe_dict['dupe_col'].append(c)
                        dupe_dict['different_vals'].append(col_rows)

            print(f'Finished {count} of {uk_len} rows...', end = '\r')
            count += 1
            
        self.dupe_data = pl.DataFrame(dupe_dict)
        return self.dupe_data

    # def dupe_data(self):
    #     if self.dupe_data != None:
    #         return self.dupe.data
    #     else:
    #         print('Sorry, this Dupe does not yet inlcude dupe date.  Run find_dupe_cols first.')
        
dup = Dupe(dfd)
dup.get_info()
dup.show_data(rows = 3)
dup.get_key_suggestion()
dup.set_key('row_id')
dupes = dup.find_dupe_cols()
dup.dupe_data
## The 'key' variable is the column for which there should be unique values.
## The entire point of this code is to identify what other columns cause the duplication
key = 'row_id'
## First, we test to see if the value is unique
total_values = df[key].shape[0]
unique_values = df[key].unique().shape[0]

total_values == unique_values
dfd
key = 'row_id'

unique_keys = df[key].unique().to_list()
uk_rows, col_rows = 0, 0
dupe_dict = {'value': [], 'val_dupes': [], 'dupe_col': []}

for uk in unique_keys[-8:]:
    uk_rows = (dfd.with_columns('row_id', pl.lit(1).alias('cnt'))
     .select(['row_id', 'cnt'])
     .filter(pl.col('row_id') == uk)
     .group_by('row_id').sum()['cnt'].item()
    )

    if uk_rows > 1:
        for c in [c for c in dfd.columns if c != key]:
            col_rows = dfd.filter(pl.col(key) == uk).select([key, c]).unique().shape[0]
            if col_rows > 1:
                dupe_dict['value'].append(uk)
                dupe_dict['val_dupes'].append(col_rows)
                dupe_dict['dupe_col'].append(c)
dupes = pl.DataFrame(dupe_dict)
dupes



time_dict = {'run': [], 'time': []}
job_start = time.time()

## We will have to go over the dataframe by the key value and then investigate the uniqueness of every column
## Where we have multiple values, we have identified columns that cause duplication and those that do not
unique_keys = df[key].unique().to_list()
rows = 0
dupe_cols = []
has_dupes = False
cur_df = pl.DataFrame()
cols = [c for c in df.columns if c != key]
results = {'key': [], 'rows': [], 'dupe_cols': []}

for uk in unique_keys:
    loop_start = time.time()
    run = 1
    ## Reset variables
    has_dupes = False
    dup_cols = ['']

    ## Create partition and capture rows
    cur_df = df.filter(pl.col(key) == uk)
    rows = cur_df.shape[0]

    # ## Test for duplicates for that key value
    # if rows > 1:
    #     for c in cols:
    #         if cur_df.select([key, c]).unique().shape[0] > 0:
    #             dupe_cols.append(c)


    results['key'].append(uk)
    results['rows'].append(rows)
    results['dupe_cols'].append(dupe_cols)
    
    end = time.time()
    time_dict['run'].append(run)
    time_dict['time'].append(end - loop_start)
    print(f'Finished {uk} with a run time of {end - loop_start}', end = '\r')
    run += 1

end = time.time()
time_dict['run'].append('total')
time_dict['time'].append(end - start)

print(f'Total job finished with runtime {end - start}')

time_dict = {'run': [], 'time': []}
job_start = time.time()

## We will have to go over the dataframe by the key value and then investigate the uniqueness of every column
## Where we have multiple values, we have identified columns that cause duplication and those that do not
unique_keys = df[key].unique().to_list()
rows = 0
dupe_cols = []
has_dupes = False
cur_df = pl.DataFrame()
cols = [c for c in df.columns if c != key]
results = {'key': [], 'rows': [], 'dupe_cols': []}

for uk in unique_keys:
    loop_start = time.time()
    run = 1
    ## Reset variables
    has_dupes = False
    dup_cols = ['']

    ## Create partition and capture rows
    cur_df = df.filter(pl.col(key) == uk)
    rows = cur_df.shape[0]

    ## Test for duplicates for that key value
    if rows > 1:
        for c in cols:
            if cur_df.select([key, c]).unique().shape[0] > 0:
                dupe_cols.append(c)


    results['key'].append(uk)
    results['rows'].append(rows)
    results['dupe_cols'].append(dupe_cols)
    
    end = time.time()
    time_dict['run'].append(run)
    time_dict['time'].append(end - loop_start)
    print(f'Finished {uk} with a run time of {end - loop_start}', end = '\r')
    run += 1

end = time.time()
time_dict['run'].append('total')
time_dict['time'].append(end - start)

print(f'Total job finished with runtime {end - start}')
(end - start) / 60
pl.DataFrame(results).tail(10)
# rdf = pl.DataFrame(results)
# rdf = rdf.explode('dupe_cols')
# rdf = rdf.filter(pl.col('rows') > 1).sort(['rows', 'key'])
# rdf.filter(pl.col('key') == 9990)
# dupe_list = rdf.filter(pl.col('rows') > 1)['dupe_cols'].unique().to_list()
# dupe_list.sort()
# dupe_list

for i in range(0, 5):
    df = pl.concat([df, df])
start = time.time()

uni = df['order_id'].unique().shape[0]

end = time.time()
print(end - start)

uni

start = time.time()

dup_keys = (df.with_columns(pl.lit(1).alias('count'))
            .select(['order_id', 'count'])
            .group_by('order_id').sum()
            .filter(pl.col('count') > 1)
           )

end = time.time()
print(end - start)
dup_keys
df.head(2)
# ## Get unique values for each column
# n_suggestions = 3

# cols = {'column': [], 'unique': []}
# uni = 0

# for c in df.columns:
#     cols['column'].append(c)
#     cols['unique'].append(df[c].unique().shape[0])

# ## Check if any columns are completely unique
# suggested_keys = []
# df_rows = df.shape[0]
# sug_key = pl.DataFrame(cols)

# if sug_key.filter(pl.col('unique') > df_rows + 1).shape[0]:
#     suggested_keys = sug_key.filter(pl.col('unique') > df_rows + 1)['column'].to_list()

# else:
#     suggested_keys = sug_key.sort('unique', descending = True).head(n_suggestions)['column'].to_list()

# suggested_keys
# ## Get unique values for each column        
# cols = {'column': [], 'unique': []}
# uni = 0

# for c in df.columns:
#     cols['column'].append(c)
#     cols['unique'].append(df[c].unique().shape[0])

# ## Check if any columns are completely unique
# suggested_keys = []
# df_rows = df.shape[0]
# sug_key = pl.DataFrame(cols)

# ## Return a list of completely unique columns
# if sug_key.filter(pl.col('unique') > df_rows + 1).shape[0]:
#     suggested_keys = sug_key.filter(pl.col('unique') > df_rows + 1)['column'].to_list()

# ## Otherwise, return the top N options based on highest unique values
# else:
#     suggested_keys = sug_key.sort('unique', descending = True).head(n_suggestions)['column'].to_list()

# print(f'Your data includes {df_rows} rows\nThe top {n_suggestions} suggestions for a unique id include:')

# ([print(f"   - {c} includes {sug_key.filter(pl.col('column') == 'row_id').select('unique').item()} unique values") 
#   for c in suggested_keys])
# n_suggestions = 3

# ## Get unique values for each column        
# cols = {'column': [], 'unique': []}
# uni = 0

# for c in df.columns:
#     cols['column'].append(c)
#     cols['unique'].append(df[c].unique().shape[0])

# ## Check if any columns are completely unique
# suggested_keys = []
# df_rows = df.shape[0]
# sug_key = pl.DataFrame(cols)

# ## Return a list of completely unique columns
# if sug_key.filter(pl.col('unique') > df_rows + 1).shape[0]:
#     suggested_keys = sug_key.filter(pl.col('unique') > df_rows + 1)['column'].to_list()

# ## Otherwise, return the top N options based on highest unique values
# else:
#     suggested_keys = sug_key.sort('unique', descending = True).head(n_suggestions)['column'].to_list()

# print(f'Your data includes {df_rows} rows\nThe top {n_suggestions} suggestions for a unique id include:')

# ([print(f"   - {c} includes {sug_key.filter(pl.col('column') == 'row_id').select('unique').item()} unique values") 
#   for c in suggested_keys])
sug_key.filter(pl.col('column') == 'row_id').select('unique').item()

