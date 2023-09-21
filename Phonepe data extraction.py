# In[1]:

get_ipython().system('pip install GitPython')

import csv
import subprocess
import pandas as pd
import requests
import git
import pandas as pd


# In[2]:


# Clone the Github repository
get_ipython().system('git clone https://github.com/PhonePe/pulse.git')

import os
import json
import pandas as pd


# In[3]:


root_dir = 'pulse/data'


# In[4]:


# Initialize empty list to hold dictionaries of data for each JSON file
data_list = []


# In[5]:


# Loop over all the state folders
state_folder = os.path.join(root_dir, 'aggregated/transaction/country/india/state')
for state_dir in os.listdir(state_folder):
    state_path = os.path.join(state_folder, state_dir)
    if os.path.isdir(state_path):

        # Loop over all the year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):

                # Loop over all the JSON files (one for each quarter)
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)

                            # Extract the data we're interested in
                            for transaction_data in data['data']['transactionData']:
                                row_dict = {
                                    'States': state_dir,
                                    'Transaction_Year': year_dir,
                                    'Quarters': int(json_file.split('.')[0]),
                                    'Transaction_Type': transaction_data['name'],
                                    'Transaction_Count': transaction_data['paymentInstruments'][0]['count'],
                                    'Transaction_Amount': transaction_data['paymentInstruments'][0]['amount']
                                }
                                data_list.append(row_dict)


# In[6]:


# Convert list of dictionaries to dataframe
df1 = pd.DataFrame(data_list)


# In[7]:


df1


# In[8]:


# Loop over all the state folders
state_folder = os.path.join(root_dir, 'aggregated/user/country/india/state')
for state_dir in os.listdir(state_folder):
    state_path = os.path.join(state_folder, state_dir)
    if os.path.isdir(state_path):

        # Loop over all the year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):

                # Loop over all the JSON files (one for each quarter)
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)

                            # Extract the data we're interested in
                            for user_data in data['data'].get('userData',[]):
                                row_dict = {
                                    'States': state_dir,
                                    'Transaction_Year': year_dir,
                                    'Quarters': int(json_file.split('.')[0]),
                                    'Device_used': user_data['name'],
                                    'Transaction_Count': user_data['count'],
                                    'Transaction_percentage': user_data['percentage']
                                      }
                                data_list.append(row_dict)


# In[9]:


# Convert list of dictionaries to dataframe
df2 = pd.DataFrame(data_list)


# In[10]:


df2


# In[11]:


root_dir = 'pulse/data/map/transaction/hover/country/india/state'

# Initialize empty list to hold dictionaries of data for each JSON file
data_list = []

# Loop over all the state folders
for state_dir in os.listdir(root_dir):
    state_path = os.path.join(root_dir, state_dir)
    if os.path.isdir(state_path):

        # Loop over all the year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):

                # Loop over all the JSON files (one for each quarter)
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)

                            # Extract the data we're interested in
                            for hoverDataList in data['data']['hoverDataList']:
                                row_dict = {
                                    'States': state_dir,
                                    'Transaction_Year': year_dir,
                                    'Quarters': int(json_file.split('.')[0]),
                                    'District': hoverDataList['name'],
                                    'Transaction_Type': hoverDataList['metric'][0]['type'],
                                    'Transaction_Count': hoverDataList['metric'][0]['amount']
                                }
                                data_list.append(row_dict)


# In[12]:


df3 = pd.DataFrame(data_list)


# In[13]:


df3


# In[14]:


root_dir = 'pulse/data/map/user/hover/country/india/state'

# Initialize empty list to hold dictionaries of data for each JSON file
data_list = []

# Loop over all the state folders
for state_dir in os.listdir(root_dir):
    state_path = os.path.join(root_dir, state_dir)
    if os.path.isdir(state_path):

        # Loop over all the year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):

                # Loop over all the JSON files (one for each quarter)
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)

                            # Extract the data we're interested in
                            for district, values in data['data']['hoverData'].items():
                                row_dict = {
                                    'States': state_dir,
                                    'Transaction_Year': year_dir,
                                    'Quarter': int(json_file.split('.')[0]),
                                    'District': district,
                                    'RegisteredUsers': values['registeredUsers'],
                                }
                                data_list.append(row_dict)


# In[15]:


# Convert list of dictionaries to dataframe
df4 = pd.DataFrame(data_list)


# In[16]:


df4 


# In[17]:


root_dir = 'pulse/data/top/transaction/country/india/state'

# Initialize empty list to hold dictionaries of data for each JSON file
data_list = []

# Loop over all the state folders
for state_dir in os.listdir(root_dir):
    state_path = os.path.join(root_dir, state_dir)
    if os.path.isdir(state_path):

        # Loop over all the year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):

                # Loop over all the JSON files (one for each quarter)
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)

                            # Extract the data we're interested in
                            for districts in data['data']['districts']:
                                row_dict = {
                                    'States': state_dir,
                                    'Transaction_Year': year_dir,
                                    'Quarters': int(json_file.split('.')[0]),
                                    'District': districts['entityName'],
                                    'Transaction_Type': districts['metric']['type'],
                                    'Transaction_Count': districts['metric']['count'],
                                    'Transaction_Amount': districts['metric']['amount']
                                }
                                data_list.append(row_dict)


# In[18]:


# Convert list of dictionaries to dataframe
df5 = pd.DataFrame(data_list)


# In[19]:


df5


# In[20]:


import os
import json
import pandas as pd

root_dir = 'pulse/data/top/user/country/india/state'

# Initialize empty list to hold dictionaries of data for each JSON file
data_list = []

# Loop over all the state folders
for state_dir in os.listdir(root_dir):
    state_path = os.path.join(root_dir, state_dir)
    if os.path.isdir(state_path):

        # Loop over all the year folders
        for year_dir in os.listdir(state_path):
            year_path = os.path.join(state_path, year_dir)
            if os.path.isdir(year_path):

                # Loop over all the JSON files
                for json_file in os.listdir(year_path):
                    if json_file.endswith('.json'):
                        with open(os.path.join(year_path, json_file)) as f:
                            data = json.load(f)

                            # Extract the data we're interested in
                            for district in data['data']['districts']:
                                row_dict = {
                                    'State': state_dir,
                                    'Transaction_Year': year_dir,
                                    'Quarters': int(json_file.split('.')[0]),
                                    'District': district['name'] if 'name' in district else district['pincode'],
                                    'RegisteredUsers': district['registeredUsers'],
                                }
                                data_list.append(row_dict)


# In[21]:


# Convert list of dictionaries to dataframe
df6 = pd.DataFrame(data_list)



# In[22]:


df6


# In[23]:


# Data transformation on file1
# Drop any duplicates
d1 = df1.drop_duplicates()
d2=df2.drop_duplicates()
d3 = df3.drop_duplicates()
d4 = df4.drop_duplicates()
d5 = df5.drop_duplicates()
d6 = df6.drop_duplicates()


# In[24]:


# Checking Null values
null_counts = d1.isnull().sum()
print(null_counts)

null_counts = d1.isnull().sum()
print(null_counts)


null_counts = d3.isnull().sum()
print(null_counts)

null_counts = d4.isnull().sum()
print(null_counts)

null_counts = d5.isnull().sum()
print(null_counts)

null_counts = d6.isnull().sum()
print(null_counts)


# In[25]:


# Converting all dataframes to CSV
d1.to_csv('agg_trans.csv', index=False)
d2.to_csv('agg_user.csv', index=False)
d3.to_csv('map_tran.csv', index=False)
d4.to_csv('map_user.csv', index=False)
d5.to_csv('top_tran.csv', index=False)
d6.to_csv('top_user.csv', index=False)


# In[26]:


# Read the CSV files
agg_trans = pd.read_csv('agg_trans.csv')
agg_user = pd.read_csv('agg_user.csv')
map_tran = pd.read_csv('map_tran.csv')
map_user = pd.read_csv('map_user.csv')
top_tran = pd.read_csv('top_tran.csv')
top_user = pd.read_csv('top_user.csv')





