from dataloader import ETL

# Load data
etl = ETL('Films_2 (3).xlsx')
etl.load_excel()  
# Get names of sheets and dfs
df_names = etl.get_names()
dfs = etl.get_dataframes()

# List of column that need cleaning
list_film = [' release_year', ' rental_duration', ' rental_rate', ' length', ' replacement_cost', ' num_voted_users']
list_inventory = [' store_id']
# Extract df
df_film = dfs[df_names.index('film')]
df_inventory = dfs[df_names.index('inventory')]
df_rental = dfs[df_names.index('rental')]
# Delete  original_language_id column
df_film = etl.drop_column(df_film, ' original_language_id')
# Clean df
dfilm_clean = etl.numeric_var_clean(df_film, list_film)
dfinventory_clean = etl.numeric_var_clean(df_inventory, list_inventory)

# Merge dfs
df1 = etl.merge_df(dfilm_clean, dfinventory_clean, 'film_id')
df_final = etl.merge_df(df1, df_rental, 'inventory_id')
df_final.to_excel('dataframe_merge.xlsx', index=False)