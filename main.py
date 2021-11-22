import pandas as pd

df = pd.read_csv ('AB_NYC_2019.csv')
#print ("Raw data:")
print (df[10:])

# groupby neighbourhoods
df_neighbourhood = df.groupby(['neighbourhood']).size().reset_index(name='counts')

df_neighbourhood.to_csv('file_name.csv', index=False)


print ("Data after postprocessing:")
print (df_neighbourhood[10:])

# # insert rows into the table
rows_to_insert = [tuple (x) for x in df_neighbourhood.to_numpy ()]
errors = client.insert_rows (table_simple_ref, rows_to_insert)
#
if errors == []:
    print ("Successfully populated the cloud DB!")
else:
    print ("Errors: ")
    print (errors)
#
destination_table = client.get_table (dataset_ref.table (table_name))
#
 print ("Loaded {} rows.".format (destination_table.num_rows))

