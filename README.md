# SOFT8033 Big Data And Analytics 2
## Installing and configuring databricks
 - `pip install --upgrade databricks-cli`
 - `databricks configure`
 - `https://community.cloud.databricks.com/`
 - `<username>`
 - `<password>`
 - `<password again>`
 
## Uploading dataset
 - `databricks fs cp -r my_dataset dbfs:/FileStore/tables/3_Assignment/my_dataset/`

## Uploading workspace
 - `databricks workspace import_dir -o my_python_spark/ /`

## Useful commands
 - `pip install --upgrade pip setuptools wheel`
 - `pip install pyspark`
