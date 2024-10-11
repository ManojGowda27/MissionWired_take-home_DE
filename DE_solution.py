# Initializing Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, col, to_date, substring

# Creating a Spark session
spark = SparkSession.builder\
        .appName("MissionWired_DE_Excercise")\
        .getOrCreate()


# Loading Data

# Loading datasets using Spark
cons_df = spark.read.csv('./Raw_Data/cons.csv', header=True, inferSchema=True)

# Extract just the date part (yyyy-MM-dd) from the custom format (e.g., 'Mon, 2021-01-01 10:00:00')
# We are extracting characters from index 6 to 15 to get the date in 'yyyy-MM-dd' format
cons_df = cons_df.withColumn('create_dt', substring('create_dt', 6, 10))

# Convert 'create_dt' to date type after extracting the relevant portion
cons_df = cons_df.withColumn('create_dt', to_date(col('create_dt'), 'yyyy-MM-dd'))

# Similarly for 'modified_dt' if needed
cons_df = cons_df.withColumn('modified_dt', substring('modified_dt', 6, 10))
cons_df = cons_df.withColumn('modified_dt', to_date(col('modified_dt'), 'yyyy-MM-dd'))

# Check if the conversion worked correctly
cons_df.printSchema()
cons_df.show(5)
email_df = spark.read.csv('./Raw_Data/cons_email.csv', header=True, inferSchema=True)
subscription_df = spark.read.csv('./Raw_Data/cons_email_chapter_subscription.csv', header=True, inferSchema=True)

# Displaying first few rows of each dataframe
cons_df.show(5)
email_df.show(5)
subscription_df.show(5)


# Data Preprocessing

# Excercise 1

# Filter subscription status for chapter_id = 1
subscription_df = subscription_df.filter(subscription_df.chapter_id == 1)

# Join email and subscription data on 'cons_email_id'
merged_df = email_df.join(subscription_df, on='cons_email_id', how='left')

# Fill null values in 'isunsub' with 0 (indicating they are still subscribed)
merged_df = merged_df.fillna({'isunsub': 0})

# Filter for primary emails (where 'is_primary' == 1)
primary_emails = merged_df.filter(merged_df.is_primary == 1)

# Join with cons_df on 'cons_id' and rename ambiguous columns
people_df = primary_emails.join(
    cons_df.withColumnRenamed('create_dt', 'cons_create_dt').withColumnRenamed('modified_dt', 'cons_modified_dt'), 
    on='cons_id', 
    how='inner'
)

# Convert 'cons_create_dt' and 'cons_modified_dt' to date format
people_df = people_df.withColumn('created_dt', to_date(people_df['cons_create_dt'], 'yyyy-MM-dd'))
people_df = people_df.withColumn('updated_dt', to_date(people_df['cons_modified_dt'], 'yyyy-MM-dd'))

# Select the necessary columns for the 'people' file:
people_df = people_df.select(
    'email', 
    col('source').alias('code'),  # Correct the alias for source
    col('isunsub').cast('boolean').alias('is_unsub'),  # Ensure isunsub is referenced correctly and cast to boolean
    'created_dt', 
    'updated_dt'
)

# Check if the conversion worked correctly
people_df.printSchema()

# Save the people file to CSV
people_df.write.csv('./Output/people', header=True)


# Excercise 2

# Group by acquisition date (using 'created_dt') and count the number of acquisitions
acquisition_facts_df = people_df.groupBy(date_format('created_dt', 'yyyy-MM-dd').alias('acquisition_date')) \
    .count().alias('acquisitions')

# Save the acquisition facts to CSV
acquisition_facts_df.write.csv('Output/acquisition_facts', header=True)

# Stop the Spark session
spark.stop() 

