from pyspark.sql import functions as F
from pyspark.sql.types import *

# --- Configuration ---
tables_string = "table_1, table_2, table_3"
volume_path = "/Volumes/catalog/schema/volume_name/my_export_folder"
exclude_col = "etl_load_ts"

# Split the string into a clean list
table_list = [t.strip() for t in tables_string.split(",")]

for table_name in table_list:
    print(f"--- Processing {table_name} ---")
    
    # 1. Load the original table
    df_original = spark.table(table_name)
    if exclude_col in df_original.columns:
        df_original = df_original.drop(exclude_col)
    
    # 2. Apply formatting for CSV export (Decimals, Dates, Timestamps)
    df_formatted = df_original
    for col_name, dtype in df_original.dtypes:
        if dtype == 'date':
            df_formatted = df_formatted.withColumn(col_name, F.date_format(F.col(col_name), "MM/dd/yyyy"))
        elif dtype == 'timestamp':
            df_formatted = df_formatted.withColumn(col_name, F.date_format(F.col(col_name), "MM/dd/yyyy HH:mm:ss"))
        elif 'decimal' in dtype or dtype in ['double', 'float']:
            df_formatted = df_formatted.withColumn(col_name, F.format_string("%.6f", F.col(col_name)))
        elif dtype in ['int', 'bigint', 'smallint']:
            df_formatted = df_formatted.withColumn(col_name, F.col(col_name).cast("string"))

    # 3. Write CSV to Volume
    temp_path = f"{volume_path}/temp_{table_name}"
    final_csv_path = f"{volume_path}/{table_name}.csv"
    
    (df_formatted.coalesce(1)
       .write
       .mode("overwrite")
       .option("header", "true")
       .option("nullValue", "") # Ensures nulls are empty strings, not 'null'
       .csv(temp_path))
    
    # Move the part-file to final CSV name and clean up temp folder
    csv_part = [f.path for f in dbutils.fs.ls(temp_path) if f.path.endswith(".csv")][0]
    dbutils.fs.cp(csv_part, final_csv_path)
    dbutils.fs.rm(temp_path, recurse=True)

    # 4. Generate SQL*Loader Control File (.ctl)
    # We use df_original.dtypes here to check the REAL types for the Oracle masks
    ctl_lines = [
        "LOAD DATA",
        f"INFILE '{table_name}.csv'",
        "APPEND", 
        f"INTO TABLE {table_name}",
        "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'",
        "TRAILING NULLCOLS",
        "("
    ]

    col_definitions = []
    for col_name, dtype in df_original.dtypes:
        if dtype == 'date':
            col_definitions.append(f'    {col_name} DATE "MM/DD/YYYY"')
        elif dtype == 'timestamp':
            col_definitions.append(f'    {col_name} TIMESTAMP "MM/DD/YYYY HH24:MI:SS"')
        else:
            col_definitions.append(f'    {col_name}')

    ctl_lines.append(",\n".join(col_definitions))
    ctl_lines.append(")")
    
    # Save the .ctl file to the Volume
    dbutils.fs.put(f"{volume_path}/{table_name}.ctl", "\n".join(ctl_lines), overwrite=True)
    
    print(f"Success: Created {table_name}.csv and {table_name}.ctl\n")
