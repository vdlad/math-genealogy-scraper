import pandas as pd
import json
from tqdm import tqdm

df = pd.read_json("data.json")

# Function to extract relevant fields
def extract_relevant_info(node):
    return {
        'id': node.get('id'),
        'name': node.get('name'),
        'advisors': node.get('advisors', []),
        'students': node.get('students', [])
    }

print("Simplifying data...")
extracted_data = [extract_relevant_info(node) for node in tqdm(df['nodes'], desc="Processing Nodes")]

filtered_df = pd.DataFrame(extracted_data)
filtered_df.to_json('relevant_data.json', orient='records')

# Create a mapping of ID to Name
id_to_name = {row['id']: row['name'] for row in extracted_data}

# Function to create relationship rows
def create_relationship_rows(row, use_names=False):
    relationships = []
    for advisor in row['advisors']:
        if advisor in id_to_name:  # Check if advisor is in the dataset
            sub = row['name'] if use_names else row['id']
            obj = id_to_name[advisor] if use_names else advisor
            relationships.append({'Subject': sub, 'Relation': 'Student', 'Object': obj})
    for student in row['students']:
        if student in id_to_name:  # Check if student is in the dataset
            sub = row['name'] if use_names else row['id']
            obj = id_to_name[student] if use_names else student
            relationships.append({'Subject': sub, 'Relation': 'Advisor', 'Object': obj})
    return relationships

print("Creating relationship table with IDs...")
relationship_data_ids = []
for _, row in tqdm(filtered_df.iterrows(), total=filtered_df.shape[0], desc="Building Relationships"):
    relationship_data_ids.extend(create_relationship_rows(row))

relationship_df_ids = pd.DataFrame(relationship_data_ids)
relationship_df_ids.drop_duplicates(inplace=True)

print("Creating relationship table with Names...")
relationship_data_names = []
for _, row in tqdm(filtered_df.iterrows(), total=filtered_df.shape[0], desc="Building Relationships"):
    relationship_data_names.extend(create_relationship_rows(row, use_names=True))

relationship_df_names = pd.DataFrame(relationship_data_names)
relationship_df_names.drop_duplicates(inplace=True)

print("Saving files...")
relationship_df_ids.to_csv('relationships_ids.csv', index=False)
relationship_df_names.to_csv('relationships_names.csv', index=False)

# Creating and saving the ID to Name mapping table
id_name_df = pd.DataFrame(list(id_to_name.items()), columns=['ID', 'Name'])
id_name_df.to_csv('id_to_name_mapping.csv', index=False)

print("All files saved successfully.")
