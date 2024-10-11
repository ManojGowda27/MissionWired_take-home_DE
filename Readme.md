# **Data Engineering Exercise - MissionWired**

### **Overview**

This repository contains the solution to the Data Engineering exercise provided by MissionWired. The task focuses on manipulating and aggregating large datasets using PySpark, with the goal of producing two CSV files:
1. A **"people"** file containing constituent information and their subscription status.
2. An **"acquisition_facts"** file that aggregates stats on the number of constituents acquired per day.

The solution demonstrates my proficiency in handling large datasets using PySpark, performing complex joins, filtering, and date handling.

### **Files in the Repository**
- **`DE_solution.py`**: The main Python script containing the code to load the datasets, process them, and generate the required output files.
- **`requirements.txt`**: Contains the dependencies required to run the PySpark environment.


---

### **Data Sources**

1. **Constituent Information (cons.csv)**: This file contains the personal information of constituents such as name, email, source code, and creation date.
2. **Constituent Email Addresses (cons_email.csv)**: This file contains email addresses linked to each constituent, specifying whether the email is primary.
3. **Constituent Subscription Status (cons_email_chapter_subscription.csv)**: This file contains information about email subscriptions.

---

### **Solution Approach**

1. **Load Datasets Using PySpark**: The datasets are read from CSV files using PySpark's `spark.read.csv()` method.
   
2. **Data Preprocessing**:
    - **Join Operations**: Constituent emails are joined with their subscription status, and then this data is linked to the constituent information.
    - **Filtering**: Primary emails are filtered, and only subscriptions with `chapter_id = 1` are retained.
   
3. **Date Handling**:
    - **Date Extraction**: One of the key challenges was handling the date fields. The `create_dt` and `modified_dt` fields were provided in a custom format such as `Day, Date, Time` (e.g., `Mon, 2021-01-01 10:00:00`). This format required extracting the `yyyy-MM-dd` portion using the `substring()` function.
    - **Conversion**: After extraction, the date fields were converted to PySpark's `date` type using the `to_date()` function for further processing.

4. **Output**:
    - **people.csv**: Contains email, source code, subscription status, and creation/modified date for each constituent.
    - **acquisition_facts.csv**: Aggregates stats about the number of constituents acquired per day.

---

### **How to Execute the Code**

To run this solution on your system, follow the steps below:

1. **Install PySpark**:
   If you don't have PySpark installed, you can install it via pip:
   ```bash
   pip install pyspark

2. **Clone the Repository**:
   Clone this repository to your local system:
   ```bash
   git clone https://github.com/ManojGowda27/MissionWired_take-home_DE.git
   cd your-repo-folder

3. **Prepare the Datasets**:
   Make sure you have the following input CSV files:
   - cons.csv - https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons.csv
   - cons_email.csv - https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email.csv
   - cons_email_chapter_subscription.csv - https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email_chapter_subscription.csv

   Place these files in the appropriate folder or path where the script can access them. Download the files from the link specified next to the dataset.

   ### **Disclaimer**

    The datasets (`cons.csv`, `cons_email.csv`, and `cons_email_chapter_subscription.csv`) used for this project have been downloaded and are being used locally. Due to GitHub's file size restrictions, these files are not included in the repository. 

    To replicate the results, please download the dataset files locally and place them in the appropriate folder (`Raw_Data/`) as referenced in the code. The structure and format of the datasets are essential for the proper execution of the solution.

4. **Run the script**:
   Execute the Python script in your local environment:
   ```bash
   spark-submit DE_solution.py 


   Alternatively, you can run it in Jupyter Notebook or Google Colab by adjusting the input paths and code format.  

5. **Check the Output**:
   - people.csv
   - acquisition_facts.csv

   This files will be saved to the working directory specified in the code.

---

### **Challenges and Pitfalls**

During the execution of the task, I faced a few challenges:

1. **Date Extraction**:
    - The `create_dt` and `modified_dt` fields in the input data were in a non-standard format (`Day, Date, Time`), which made it difficult to directly use them for aggregation.
    - **Solution**: I used the `substring()` function to extract only the `yyyy-MM-dd` portion of the date and then converted it to PySpark's `date` type for further processing.

2. **Handling Null Values**:
    - The `isunsub` field had null values for some emails, where no subscription information was present. This needed to be handled carefully to ensure those records were treated as subscribed.
    - **Solution**: I used PySpark's `fillna()` method to replace null values with `0`, indicating that the email is subscribed.

3. **Performance**:
    - While working with large datasets, it’s important to ensure efficient memory management and processing. PySpark handles large datasets well, but using operations like `join`, `groupBy()`, and `filter` can slow down performance if not optimized.

4. **Ambiguous Column Names**:
    - After some joins, there were multiple columns with the same name, which caused ambiguity during the selection of columns.
    - **Solution**: I used column renaming to disambiguate the columns and ensure that the correct fields were selected for further processing.

---

### **Requirements**

To run this solution, you will need the following dependencies:

- **PySpark** (version 3.0 or higher)
- **Python 3.x**
- **Numpy**
- (Optional) **Jupyter** or **Google Colab** for interactive execution

To install the required libraries, run:

```bash
pip install -r requirements.txt
