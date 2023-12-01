#!/usr/bin/env python
# coding: utf-8

# In[6]:


#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql import Row
import csv
from pyspark import SparkConf

# Set HDFS URL using the spark.hadoop prefix
conf = SparkConf().setAppName("YourSparkApplication").setMaster("spark://192.168.250.148:7077")
conf.set("spark.hadoop.fs.default.name", "hdfs://0.0.0.0:19000")

# Create a Spark session
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Initialize an empty list to store the data
data = []

# Read data from the CSV file
with open("C:\\Users\\Pooja Shah\\OneDrive\\Desktop\\bdata_dataset.csv", 'r') as file:
    reader = csv.reader(file)
    # Skip the header row if it exists
    next(reader, None)
    
    # Iterate over rows and append to the data list
    for row in reader:
        data.append(tuple(row))

# Print the resulting data
print(data)

# Define schema
columns = ["name", "year", "selling_price", "km_driven", "fuel", "seller_type","transmission","owner"]
schema = spark.createDataFrame([Row(*data[i]) for i in range(len(data))], columns).schema

# Create a DataFrame
pysparkDF = spark.createDataFrame(data=data, schema=schema)

# Save DataFrame to Hive
pysparkDF.write.mode("overwrite").saveAsTable("car_dataset_18")

# Show DataFrame schema and data
pysparkDF.printSchema()
pysparkDF.show(truncate=False)


# In[ ]:


import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import pandas as pd

hive_table_name = "car_dataset_18"  # Replace with your actual Hive table name
hiveDF = spark.sql(f"SELECT * FROM {hive_table_name}")

# Assuming you have a column named "fuel" in your DataFrame
fuel_counts = hiveDF.groupBy("fuel").count().toPandas()

# Assuming you have columns named "year" and "selling_price"
years_data = hiveDF.groupBy("year").count().toPandas()
selling_price_data = hiveDF.groupBy("selling_price").count().toPandas()

# Create a figure with subplots
fig, axs = plt.subplots(2, 2, figsize=(12, 8))

# Plotting Pie Chart
axs[0, 0].pie(fuel_counts["count"], labels=fuel_counts["fuel"], autopct='%1.1f%%')
axs[0, 0].set_title("Fuel Distribution")

# Plotting Line Chart
axs[0, 1].plot(years_data["year"], years_data["count"], marker='o')
axs[0, 1].set_title('Line Chart - Year Distribution')
axs[0, 1].set_xlabel('Year')
axs[0, 1].set_ylabel('Count')

# Plotting Bar Chart
axs[1, 0].bar(selling_price_data["selling_price"], selling_price_data["count"])
axs[1, 0].set_title('Bar Chart - Selling Price Distribution')
axs[1, 0].set_xlabel('Selling Price')
axs[1, 0].set_ylabel('Count')

# Remove empty subplot
fig.delaxes(axs[1, 1])

# Show the plots
plt.tight_layout()
plt.show()

