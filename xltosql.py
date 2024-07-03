import pandas as pd
import os
import re
import mysql.connector
import json
from datetime import datetime

config_df = pd.read_excel("configuration.xlsx")  # Read the input configuration Excel


def read_excel_with_config(dir_path, file_list, key, sheet_config, filename_regex) -> None | pd.DataFrame:
    master_df = pd.DataFrame()

    # Grab Files from directory
    for filename in file_list:
        df = None

        found_file = re.search(filename_regex, filename)  # Check if file name matches the regex
        if found_file is None:
            print(f"File is not matched with regex: {filename_regex} | file: {filename}")
            continue
        else:
            print(f"File is matched with regex: {filename_regex} | file: {filename}")

        file_path = os.path.join(dir_path, found_file.group(0))
        print("file_path:", file_path)

        try:  # Loop through selected tabs and read only selected fields
            for tab in sheet_config["sheets"]:
                try:
                    print("tab:", tab)
                    # IF the config's regex matches the file name
                    # Read the file

                    try:
                        df_new = pd.read_excel(file_path, sheet_name=tab["name"], usecols=tab["fields"])

                    except Exception as e:
                        print(
                            "Could not read file with tab name and fields:",
                            file_path,
                            tab["name"],
                            tab["fields"],
                        )

                    if df is None:  # Merge dataframes based on KEY if provided
                        print("Creating initial dataframe")
                        df = df_new  # Only runs for the first tab
                    elif type(key) is str and key != "nan":
                        print("Joining with defined key:", key)
                        df = pd.merge(df, df_new, on=key, how="left")
                    else:
                        print("Joining with no defined key...")
                        df = pd.merge(df, df_new)  # If no key is provided, merge on all columns

                    print("New Dataframe Shape:", df.shape)

                except Exception as e:
                    print(f"Error reading {file_path} with sheet {tab}. Error: {e}\n")

                    print(
                        "The Sheet names avalible are:",
                        pd.ExcelFile(file_path).sheet_names,
                        "\n\n",
                    )
                    continue

        except Exception as e:
            print(f"Error reading {tab} with sheet_config {sheet_config[0]}. Error: {e} \n\n")
            continue

        print("===========================================")
        master_df = pd.concat([master_df, df], axis=0)

    return master_df


for _, row in config_df.iterrows():  # Loop through each row in the configuration file
    if os.path.exists(row["DIRECTORY_PATH"]):  # Check if directory path exists
        directory_path = os.path.normpath(row["DIRECTORY_PATH"])
    else:
        raise Exception("Directory path does not exist")

    filename_regex = row["FILENAME_REGEX"]
    key = str(row["KEY"])
    print("key:", key, type(key))
    sheet_config: dict = json.loads(row["SELECTED_TABS"])

    for directory_path, _, file_list in os.walk(directory_path):
        print(f"Processing files: {directory_path} - {file_list}")

        df = read_excel_with_config(directory_path, file_list, key, sheet_config, filename_regex)
        print("Final Dataframe Shape:", df.shape)


# df.to_excel("./output.xlsx")


# Parsing database credentials
db_credentials = {"host": "", "port": "", "user": "", "password": ""}
db_credentials = json.loads(config_df["DATABASE_CREDENTIALS"][0])

connection = mysql.connector.connect(
    host=db_credentials["host"],
    port=db_credentials["port"],
    database=config_df["TARGET_DATABASE"][0],
    user=db_credentials["user"],
    password=db_credentials["password"],
)

cursor = connection.cursor()
# Create a unique table name based on the KEY and current datetime
table_name = "MRUM_DEV" + datetime.now().strftime("%Y%m%d%H%M%S")


field_definitions = []  # Analyze data types of each selected field and map to MySQL types
for column, dtype in df.dtypes.items():
    if "int" in str(dtype):
        mysql_type = "INT"
    elif "float" in str(dtype):
        mysql_type = "FLOAT"
    elif "datetime" in str(dtype):
        mysql_type = "DATETIME"
    else:
        mysql_type = "VARCHAR(255)"  # Default to VARCHAR with a max length, you can also refine this based on unique data analysis
    field_definitions.append(f"{column} {mysql_type}")


field_definitions_str = ", ".join(field_definitions)
# Create a new table in the database using the generated field definitions
create_table_query = f"CREATE TABLE {table_name} ({field_definitions_str})"
print(create_table_query)

cursor.execute(create_table_query)
# Insert data into the newly created table
columns_str = ", ".join(df.columns)
placeholders = ", ".join(["%s"] * len(df.columns))
insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

for _, row in df.iterrows():
    cursor.execute(insert_query, tuple(row))

connection.commit()
cursor.close()
connection.close()

print(f"Data transferred to table: {table_name}!")
