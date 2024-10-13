# **MissionWired Data Engineer Exercise - Test Suite**

## **Overview**

This folder contains the test suite to validate the ETL pipeline for the Data Engineer exercise. The tests are written using **Pytest** and are designed to ensure that the different stages of the ETL process (Extraction, Transformation, Loading) work correctly.

### **Key Tests:**
1. **Data Loading**: Verifies that the CSV files are loaded correctly, and the schema matches the expected structure.
2. **Date Formatting**: Checks that the custom date formats are correctly transformed to the `yyyy-MM-dd` format.
3. **Data Joins**: Ensures that the join operations between DataFrames are performed correctly.
4. **Aggregation**: Validates that the acquisitions per day are aggregated accurately.
5. **CSV Writing**: Confirms that the final DataFrames are written to CSV files as expected.

---

## **Setup**

### **1. Clone the Repository**
Clone this repository to your local machine:

```bash
git clone https://github.com/your-repo-url/MissionWired_DE.git
cd MissionWired_DE
```

### **2. Install Dependencies**

You need to install the dependencies required for the tests. This includes **PySpark** and **Pytest**.

To install the dependencies, use the following commands:

```bash
pip install pyspark
pip install pytest
```

### **3. Prepare the Dataset**

Make sure you have the necessary CSV files (`cons.csv`, `cons_email.csv`, `cons_email_chapter_subscription.csv`) in the `../Raw_Data/` folder. These files are required for the tests to run properly.

---

## **Running the Tests**

To run the test suite, simply execute the following command from the project root:

```bash
pytest DE_solution_test.py
```

Pytest will automatically discover and run all the tests in the `DE_solution_test.py` file.

### **Test Output**

You will see the test results in the terminal, showing which tests passed and if any failed. For example:

```bash
collected 5 items

DE_solution_test.py .F...                                                                                                               [100%]
```

If all tests pass, the output will show `100%` successful completion.

---

## **Test Descriptions**

### **1. Data Loading (`test_data_loading`)**
- **Purpose**: To check if the CSV files are loaded correctly and the schema matches the expected column structure.
- **Validation**: Ensures that the DataFrame is not empty and all required columns are present in the correct order.

### **2. Date Formatting (`test_date_formatting`)**
- **Purpose**: To verify that custom date strings (e.g., `Mon, 2021-01-01 10:00:00`) are correctly converted into `yyyy-MM-dd` format.
- **Validation**: Ensures that no `null` values remain in the `create_dt` and `modified_dt` fields after transformation.

### **3. Data Join (`test_join`)**
- **Purpose**: To validate that the join operations between constituent, email, and subscription DataFrames work as expected.
- **Validation**: Checks that the correct number of rows are returned after the `inner` join.

### **4. Aggregation (`test_aggregation`)**
- **Purpose**: To ensure that the acquisitions per day are calculated correctly.
- **Validation**: Verifies that the aggregation results (counts per day) are accurate.

### **5. CSV Writing (`test_write_csv`)**
- **Purpose**: To validate that DataFrames are correctly written to CSV files.
- **Validation**: Ensures that the files are created in the specified directory and are not empty.

---

## **Troubleshooting**

If you encounter issues or failing tests, check the following:
1. **Data Files**: Ensure the CSV files are located in the correct directory (`../Raw_Data/`) and are structured properly.
2. **Python Version**: This test suite is designed for Python 3. Make sure you are using Python 3.x (preferably Python 3.7+).
3. **PySpark Compatibility**: Ensure that PySpark is installed and correctly configured.
