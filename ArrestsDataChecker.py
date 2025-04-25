import duckdb
import os

duckdb = duckdb.connect()

csv_path = 'Arrest2010.csv' 
assert os.path.exists(csv_path), f"File not found: {csv_path}"

duckdb.execute(f"""
CREATE OR REPLACE TABLE arrests2020 AS
SELECT * From read_csv_auto('{csv_path}');
""")

total_ids = duckdb.execute('Select COUNT("Report ID") From arrests2020').fetchone()[0]
unique_ids = duckdb.execute('Select COUNT(DISTINCT "Report ID") From arrests2020').fetchone()[0]
assert total_ids == unique_ids, f"'Report ID' is not unique: {total_ids} total vs {unique_ids} unique"

null_arrest_date = duckdb.execute('Select COUNT(*) From arrests2020 Where "Arrest Date" IS NULL').fetchone()[0]
assert null_arrest_date == 0, f"Missing 'Arrest Date': {null_arrest_date} rows"

invalid_time = duckdb.execute('Select COUNT(*) From arrests2020 Where Time IS NULL').fetchone()[0]
print(f"Null Time values: {invalid_time} rows")

invalid_area = duckdb.execute('Select COUNT(*) From arrests2020 Where "Area ID" IS NULL').fetchone()[0]
print(f"Null Area ID values: {invalid_area} rows")

null_area_name = duckdb.execute('Select COUNT(*) From arrests2020 Where "Area Name" IS NULL').fetchone()[0]
assert null_area_name == 0, "Missing Area Name"

invalid_age = duckdb.execute('Select Count (Age) From arrests2020 Where Age = 0;').fetchone()[0]
print("The number of people with age 0 is: " + str(invalid_age))

impossible_age = duckdb.execute('SELECT Count(*) From arrests2020 Where Age < 0 OR Age > 120').fetchone()[0]
print(f"Invalid victim ages (<0 or >120): {impossible_age} rows")

distinct_sex = duckdb.execute('Select DISTINCT "Sex Code" From arrests2020').fetchdf()
print("Distinct Sex Codes:", distinct_sex)

null_descent = duckdb.execute('Select COUNT(*) From arrests2020 Where "Descent Code" IS NULL').fetchone()[0]
assert null_descent == 0, f"Missing Descent Code: {null_descent} rows"

distinct_arrest_type = duckdb.execute('Select DISTINCT "Arrest Type Code" From arrests2020').fetchdf()
print("Distinct Arrest Type Codes:", distinct_arrest_type)

null_charge = duckdb.execute('Select COUNT(*) From arrests2020 Where Charge IS NULL').fetchone()[0]
print("Missing Charge or Description in " + str(null_charge) + " rows")

null_dispo = duckdb.execute('Select COUNT(*) From arrests2020 Where "Disposition Description" IS NULL').fetchone()[0]
print(f"Missing Disposition Description: {null_dispo} rows")

null_address = duckdb.execute('Select COUNT(*) From arrests2020 Where Address IS NULL').fetchone()[0]
print(f"Missing Address: {null_address} rows")

invalid_lat = duckdb.execute('Select COUNT(*) From arrests2020 Where LAT IS NULL OR LAT < 30 OR LAT > 40').fetchone()[0]
invalid_lon = duckdb.execute('Select COUNT(*) From arrests2020 Where LON IS NULL OR LON > -110 OR LON < -120').fetchone()[0]
print(f"Invalid or missing LAT: {invalid_lat}")
print(f"Invalid or missing LON: {invalid_lon}")

null_location = duckdb.execute('Select COUNT(*) From arrests2020 Where Location IS NULL').fetchone()[0]
print(f"Missing Location: {null_location} rows")

