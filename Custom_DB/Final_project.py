import os
import re
import pandas as pd
import mysql.connector
import subprocess 


print("Welcome!")
parquet_dir = input("Please input the directory of the folder where the Parquet files are present: ")



def convert_parquet_to_csv(parquet_dir):
    csv_files = []
    for filename in os.listdir(parquet_dir):
        if filename.endswith(".parquet"):
            file_path = os.path.join(parquet_dir, filename)
            df = pd.read_parquet(file_path)
            csv_filename = filename.replace(".parquet", ".csv")
            csv_file_path = os.path.join(parquet_dir, csv_filename)
            df.to_csv(csv_file_path, index=False)
            csv_files.append(csv_file_path)
            print(f"Converted {filename} to {csv_filename}")
    return csv_files


def clean_and_flatten_csv(csv_files):
    cleaned_files = []
    for csv_file in csv_files:
        df = pd.read_csv(csv_file, low_memory=False)

        def clean_string_cell(cell):
            if isinstance(cell, str):
                cell = re.sub(r'[\"\[\]\']', '', cell)
            return cell

        for col in df.select_dtypes(include=['object']).columns:
            if col != 'npis':
                df[col] = df[col].apply(clean_string_cell)

        if 'npis' in df.columns:
            df['npis'] = df['npis'].apply(lambda x: re.sub(r"[\[\]']", '', str(x)).strip())
            df['npis'] = df['npis'].apply(lambda x: x.split() if x else [])
            df['npis'] = df['npis'].apply(lambda x: [item for item in x if item.isdigit()])
            df = df.explode('npis')
            df.loc[df['npis'] == '', 'npis'] = pd.NA

        df.to_csv(csv_file, index=False, na_rep='NULL')
        cleaned_files.append(csv_file)
        print(f"Flattening complete. Output saved to '{csv_file}'.")
    return cleaned_files


def create_databases(csv_files):
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="password",
        allow_local_infile=True
    )

    cursor = conn.cursor()
    cursor.execute("SET GLOBAL local_infile = 1;")
    cursor.execute("CREATE DATABASE IF NOT EXISTS project_work2")
    cursor.execute("USE project_work2")

    for csv_file in csv_files:
        table_name = os.path.basename(csv_file).replace(".csv", "")
        df = pd.read_csv(csv_file, low_memory=False)
        columns = ', '.join([f"`{col}` TEXT" for col in df.columns])

        create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({columns});"
        cursor.execute(create_table_query)

        load_data_query = f"""
            LOAD DATA LOCAL INFILE '{csv_file.replace("\\", "\\\\")}'
            INTO TABLE `{table_name}`
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"';
        """
        try:
            cursor.execute(load_data_query)
            conn.commit()
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            continue

        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        row_count = cursor.fetchone()[0]
        print(f"Data loaded successfully. The table '{table_name}' now contains {row_count} rows.")

    cursor.close()
    conn.close()


csv_files = convert_parquet_to_csv(parquet_dir)
cleaned_csv_files = clean_and_flatten_csv(csv_files)
create_databases(cleaned_csv_files)




def create_backup():
    back_up_dir = parquet_dir
    backup_filename = "backup.sql"
    back_up_filepath = os.path.join(back_up_dir, backup_filename)

    host = "localhost"
    user = "root"
    password = "password"
    database = "project_work2"

    try:
        subprocess.run(
            ["mysqldump", "-h", host, "-u", user, f"--password={password}", database, "-r", back_up_filepath],
            check=True
        )
        print(f"Database '{database}' backed up successfully to '{back_up_filepath}'.")
    except subprocess.CalledProcessError as e:
        print(f"Error during backup: {e}")

create_backup()