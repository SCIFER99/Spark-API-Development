# By: Tim Tarver
# Developing a basic Spark API

# Import all modules required to develop a Spark API

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark Session

spark = SparkSession.builder.appName('SparkAPI').getOrCreate()

# Define the API functions with specified file path

def read_and_process_data(file_path):
    # Read the CSV Data
    data = spark.read.csv(file_path, header=True, inferSchema=True)

    # Perform Transformations
    data_transform = data.select(col('Name'), col('Age') + 1)

    return data_transform

if __name__ == '__main__':
    input_file = 'titanic.csv'
    data_results = read_and_process_data(input_file)
    data_results.show()

    # Stop the Spark session
    spark.stop()
