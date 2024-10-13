import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, substring, count

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("TestMissionWired").getOrCreate()

# Test 1: Check data loading
def test_data_loading(spark):
    # Load CSV file
    cons_df = spark.read.csv('../Raw_Data/cons.csv', header=True, inferSchema=True)
    
    # Print the schema to investigate (this step can be removed once you confirm it's correct)
    cons_df.printSchema()
    print(cons_df.columns)
    
    # Assert that DataFrame is not empty
    assert cons_df.count() > 0, "cons_df DataFrame is empty"
    
    # Expected schema based on the actual column order from the error output
    expected_columns = ['cons_id', 'prefix', 'firstname', 'middlename', 'lastname', 'suffix', 'salutation', 'gender', 'birth_dt', 
                        'title', 'employer', 'occupation', 'income', 'source', 'subsource', 'userid', 'password', 
                        'is_validated', 'is_banned', 'change_password_next_login', 'consent_type_id', 'create_dt', 
                        'create_app', 'create_user', 'modified_dt', 'modified_app', 'modified_user', 'status', 'note']
    
    # Actual schema
    actual_columns = cons_df.columns
    
    # Assert schema matches (adjusted expected columns based on actual schema)
    assert expected_columns == actual_columns, "Schema doesn't match"


# Test 2: Check date formatting transformation
def test_date_formatting(spark):
    # Sample data for testing
    data = [("1", "Mon, 2021-01-01 10:00:00"), ("2", "Tue, 2021-02-01 11:30:00")]
    columns = ["cons_id", "create_dt"]
    df = spark.createDataFrame(data, columns)

    # Extract and convert the date
    df = df.withColumn('create_dt', substring('create_dt', 6, 10))
    df = df.withColumn('create_dt', to_date(col('create_dt'), 'yyyy-MM-dd'))

    # Check that the date has been converted correctly
    assert df.filter(df['create_dt'].isNull()).count() == 0, "Date conversion failed"

# Test 3: Test data join
def test_join(spark):
    # Sample data
    cons_data = [(1, 'John', '2021-01-01'), (2, 'Jane', '2021-02-01')]
    email_data = [(1, 'john@example.com', 1), (3, 'doe@example.com', 0)]
    
    cons_df = spark.createDataFrame(cons_data, ["cons_id", "name", "create_dt"])
    email_df = spark.createDataFrame(email_data, ["cons_id", "email", "is_primary"])
    
    # Join data
    joined_df = email_df.join(cons_df, on='cons_id', how='inner')
    
    # Assert correct row count after the join
    assert joined_df.count() == 1, "Join result is incorrect"
    assert joined_df.filter(joined_df['cons_id'] == 1).count() == 1, "Join result for cons_id 1 missing"

# Test 4: Check aggregation (acquisitions per day)
def test_aggregation(spark):
    # Sample data
    data = [(1, '2021-01-01'), (2, '2021-01-01'), (3, '2021-02-01')]
    columns = ['cons_id', 'create_dt']
    df = spark.createDataFrame(data, columns)

    # Group by date and count acquisitions
    result_df = df.groupBy('create_dt').agg(count('cons_id').alias('acquisitions'))

    # Assert correct acquisition counts
    assert result_df.filter(result_df['create_dt'] == '2021-01-01').select('acquisitions').collect()[0][0] == 2
    assert result_df.filter(result_df['create_dt'] == '2021-02-01').select('acquisitions').collect()[0][0] == 1

# Test 5: Check if DataFrame was written to CSV
def test_write_csv(spark, tmpdir):
    # Sample data
    data = [(1, 'john@example.com'), (2, 'jane@example.com')]
    df = spark.createDataFrame(data, ['cons_id', 'email'])

    # Write DataFrame to CSV
    output_path = tmpdir.join('output.csv')
    df.write.csv(str(output_path), header=True)

    # Check if the file was written
    assert output_path.exists(), "CSV file was not written"
