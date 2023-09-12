import pandas as pd

# Instead of uploading, directly read from your local directory
# Replace 'path_to_file' with the local path where your file is stored
df_dict = {
    "paylocity_df": pd.read_csv('path_to_paylocity_file.csv'),
    "q1_df": pd.read_csv('path_to_q1_file.csv'),
    "q2_df": pd.read_csv('path_to_q2_file.csv')
}

paylocity_df = df_dict.get("paylocity_df")
q1_df = df_dict.get("q1_df")
q2_df = df_dict.get("q2_df")



q1_df

# q1_df['Survey Metadata - End Date (+00:00 GMT)'] = pd.to_datetime(q1_df['Survey Metadata - End Date (+00:00 GMT)'])

# # Determine the most recent month
# latest_month = q1_df['Survey Metadata - End Date (+00:00 GMT)'].dt.to_period('M').max()

# # Filter the data to only include rows from the most recent month
# latest_data = q1_df[q1_df['Survey Metadata - End Date (+00:00 GMT)'].dt.to_period('M') == latest_month]
# latest_data





# Merge the dataframes on the specified columns
paylocity_df.rename(columns={'CostCenter1': 'LocCode'}, inplace=True)
merged_df = q2_df.merge(paylocity_df[['EmployeeID', 'LocCode']],
                        left_on='External Data Reference',
                        right_on='EmployeeID',
                        how='left')

# Drop the 'EmployeeID' column after merging as it's redundant
merged_df.drop('EmployeeID', axis=1, inplace=True)

print()
# Now, group by "External Data Reference" and "CostCenter2"
grouped = merged_df.groupby(['External Data Reference', 'LocCode']).size().reset_index(name='count')

# Group by community and then aggregate to get the sum
sum_counts_by_community = merged_df.groupby('LocCode').size().reset_index(name='sum_counts')

print(sum_counts_by_community)

q1_df

# Duplicate the column
q1_df['LocCode'] = q1_df['Embedded Data - Community']

# Now, group by 'LocCode' and aggregate
aggregated = q1_df.groupby('LocCode').agg({
    'Q1 - On a scale of 0-10 how satisfied are you working for Morning Pointe?': 'mean',
    'Q2 - What can we do better?': 'count',
    'Embedded Data - Community': 'size'
}).reset_index()

# Round the Q1 average score to 1 decimal place
aggregated['Q1 - On a scale of 0-10 how satisfied are you working for Morning Pointe?'] = aggregated['Q1 - On a scale of 0-10 how satisfied are you working for Morning Pointe?'].round(1)

# Rename the columns for clarity
aggregated.columns = ['LocCode', 'Average Q1 Score', 'Count of Q2 Entries', 'Count of Community Entries']

print(aggregated)

# # Group by 'Embedded Data - Community' and aggregate
# aggregated = q1_df.groupby('Embedded Data - Community').agg({
#     'Q1 - On a scale of 0-10 how satisfied are you working for Morning Pointe?': 'mean',
#     'Q2 - What can we do better?': 'count'
# }).reset_index()

# # Round the Q1 average score to 1 decimal place
# aggregated['Q1 - On a scale of 0-10 how satisfied are you working for Morning Pointe?'] = aggregated['Q1 - On a scale of 0-10 how satisfied are you working for Morning Pointe?'].round(1)

# # Rename the columns for clarity
# aggregated.columns = ['Embedded Data - Community', 'Average Q1 Score', 'Count of Q2 Entries']

# print(aggregated)

# Assuming the 'sum_counts_by_community' DataFrame has a column named 'community' for the community names.
combined = pd.merge(aggregated, sum_counts_by_community, left_on='LocCode', right_on='LocCode', how='left')

print(combined)

# Assuming you've already calculated sum_counts for each community
# and it's present in the 'sum_counts_by_community' DataFrame.

# Merge the two dataframes on 'LocCode' (or community name)
merged_results = aggregated.merge(sum_counts_by_community, on='LocCode', how='left')

# Calculate the % of completion and round to 2 decimals
merged_results['% Completion'] = ((merged_results['Count of Community Entries'] / merged_results['sum_counts']) * 100).round(2)

print(merged_results)
merged_results.to_csv('filename.csv')
