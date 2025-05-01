from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Cleaning") \
    .getOrCreate()

# Load data
input_path = "path/to/your/input/data.csv"
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Data cleaning steps

# 1. Remove duplicates
df = df.dropDuplicates()

# 2. Handle missing values
# Replace nulls in numeric columns with 0
numeric_columns = [field.name for field in df.schema.fields if str(field.dataType) in ['IntegerType', 'DoubleType']]
for col_name in numeric_columns:
    df = df.fillna({col_name: 0})

# Replace nulls in string columns with 'Unknown'
string_columns = [field.name for field in df.schema.fields if str(field.dataType) == 'StringType']
for col_name in string_columns:
    df = df.fillna({col_name: 'Unknown'})

# 3. Trim whitespace from string columns
for col_name in string_columns:
    df = df.withColumn(col_name, trim(col(col_name)))

# 4. Standardize categorical values (example: gender column)
if 'gender' in df.columns:
    df = df.withColumn('gender', when(col('gender').isin('Male', 'Female'), col('gender')).otherwise('Unknown'))

# 5. Remove invalid rows (example: age < 0)
if 'age' in df.columns:
    df = df.filter(col('age') >= 0)

# Save cleaned data
output_path = "path/to/your/output/cleaned_data.csv"
df.write.csv(output_path, header=True, mode='overwrite')

# Stop Spark session
spark.stop()