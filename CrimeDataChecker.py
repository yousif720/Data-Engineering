import duckdb
import os

duckdb = duckdb.connect()

csv_path = 'Crime2010.csv'
assert os.path.exists(csv_path), f"File not found: {csv_path}"

duckdb.execute(f"""
CREATE OR REPLACE TABLE crime2020 AS
SELECT * From read_csv_auto('{csv_path}');
""")

total_primary_keys = duckdb.execute("Select count (DR_NO) From crime2020").fetchone()[0]
unique_primary_keys = duckdb.execute("Select count (distinct DR_NO) From crime2020").fetchone()[0]
assert total_primary_keys == unique_primary_keys, f"Expected them to be the same, there are {total_primary_keys} total while only {unique_primary_keys} unique."

invalid_age = duckdb.execute('Select Count ("Vict Age") From crime2020 Where "Vict Age" = 0;').fetchone()[0]
print("The number of people with age 0 is: " + str(invalid_age))

impossible_age = duckdb.execute('SELECT Count(*) From crime2020 Where "Vict Age" < 0 OR "Vict Age" > 120').fetchone()[0]
print(f"Invalid victim ages (<0 or >120): {impossible_age} rows")

null_date_rptd = duckdb.execute('Select Count(*) From crime2020 Where "Date Rptd" IS NULL').fetchone()[0]
null_date_occ = duckdb.execute('Select Count(*) From crime2020 Where "DATE OCC" IS NULL').fetchone()[0]
assert null_date_rptd == 0, f"Missing values in 'Date Rptd': {null_date_rptd}"
assert null_date_occ == 0, f"Missing values in 'DATE OCC': {null_date_occ}"


invalid_time = duckdb.execute('Select Count(*) From crime2020 Where "TIME OCC" IS NULL').fetchone()[0]
assert invalid_time == 0, f"Empty Time in': {invalid_time} rows"

null_area_name = duckdb.execute('Select Count(*) From crime2020 Where "AREA NAME" IS NULL').fetchone()[0]
assert null_area_name == 0, f"Missing AREA NAME: {null_area_name} rows"

unexpected_parts = duckdb.execute('Select Count(*) From crime2020 Where "Part 1-2" NOT IN (1, 2)').fetchone()[0]
assert unexpected_parts == 0, f"Unexpected 'Part 1-2' values: {unexpected_parts} rows"

null_crm_cd = duckdb.execute('Select Count(*) From crime2020 Where "Crm Cd" IS NULL').fetchone()[0]
assert null_crm_cd == 0, f"Missing 'Crm Cd': {null_crm_cd} rows"

null_desc = duckdb.execute('Select Count(*) From crime2020 Where "Crm Cd Desc" IS NULL').fetchone()[0]
assert null_desc == 0, f"Missing 'Crm Cd Desc': {null_desc} rows"

sex_values = duckdb.execute('Select DISTINCT "Vict Sex" From crime2020').fetchall()
print(f"Unique values in Vict Sex: {sex_values}")

null_descent = duckdb.execute('Select Count(*) From crime2020 Where "Vict Descent" IS NULL').fetchone()[0]
print(f"Missing victim descent values: {null_descent} rows")


null_premis_desc = duckdb.execute('Select Count(*) From crime2020 Where "Premis Desc" IS NULL').fetchone()[0]
print( f"Missing 'Premis Desc': {null_premis_desc} rows")

weapon_codes = duckdb.execute('Select Count(*) From crime2020 Where "Weapon Used Cd" IS NOT NULL').fetchone()[0]
print(f"Rows with weapon used code: {weapon_codes}")

null_status = duckdb.execute('Select Count(*) From crime2020 Where Status IS NULL').fetchone()[0]
print(f"Missing 'Status': {null_status} rows")

crm_cd_cols = ["Crm Cd 1", "Crm Cd 2", "Crm Cd 3", "Crm Cd 4"]
for col in crm_cd_cols:
    invalid = duckdb.execute(f'Select Count(*) From crime2020 Where "{col}" < 100 OR "{col}" > 9999').fetchone()[0]
    print(f"Out-of-range or missing values in {col}: {invalid}")

null_location = duckdb.execute('Select Count(*) From crime2020 Where LOCATION IS NULL').fetchone()[0]
print(f"Missing LOCATION: {null_location} rows")

invalid_lat = duckdb.execute('Select Count(*) From crime2020 Where LAT IS NULL').fetchone()[0]
invalid_lon = duckdb.execute('Select Count(*) From crime2020 Where LON IS NULL').fetchone()[0]
print(f"Missing LAT: {invalid_lat}")
print(f"Missing LON: {invalid_lon}")