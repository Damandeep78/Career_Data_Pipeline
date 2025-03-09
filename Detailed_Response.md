# Career Analytics Data Model and Report

## Part 1: Data Engineering

### 1. ETL Process Design:

* **Extraction:**
    * Read the `professionals_nested.json` file.
    * Parse the JSON data into a Python dictionary.
* **Transformation:**
    * Iterate through the `professionals` list in the JSON data.
    * For each professional, extract data and append it to lists for professionals, jobs, skills, certifications, and educations.
    * Create Pandas DataFrames from these lists.

* **Validation:**
    * Convert date columns to datetime objects.
    * Check for job end dates before start dates.
     * Handle null values in date fields.
    * Handle null values in the `years_experience` and `end_date` fields.
    * Ensure all data types are correct.
 
* **Loading:**
    * Load the transformed DataFrames into DuckDB tables.
    * Create a dimensional model with fact and dimension tables, with primary and foreign keys.
    * Add features like  Job Duration (end date - start date), Current Job flag (Y/N) in the Fact table.

### 2. Data Validation and Quality Checks (Detailed):

* **Date Inconsistencies:**
    * Convert date columns to datetime objects using `pd.to_datetime()`.
    * Implement checks to ensure `end_date` is not before `start_date` in the `jobs` DataFrame.
    * Handle parsing errors and invalid date formats.
* **Missing Data:**
    * Use `df.fillna()` or `df.dropna()` to handle null values in `years_experience` and `end_date`.
    * Implement logic to handle missing `end_date` values (e.g., set to current date or a placeholder).
* **Data Type Validation:**
    * Use `df.dtypes` to check data types.
    * Convert columns to appropriate types using `df.astype()`.
    * Add validation for professional\_id, job\_id, skill\_id, certification\_id, and education\_id to ensure they are unique.

### 3. Dimensional Model:

* **Fact Table:** `FactJobs` (job\_id, professional\_id, company, industry, role, start\_date, end\_date, salary\_band, job\_duration)
* **Dimension Tables:**
    * `DimProfessionals` (professional\_id, years\_experience)
    * `DimSkills` (skill\_id, professional\_id, skill\_name, years\_experience)
    * `DimCertifications` (certification\_id, professional\_id, date\_earned, expiration\_date)
    * `DimEducations` (education\_id, professional\_id, graduation\_date)
* **Primary and Foreign Keys:**
    * `DimProfessionals`: `professional_id` (PK)
    * `DimSkills`: `skill_id` (PK), `professional_id` (FK)
    * `DimCertifications`: `certification_id` (PK), `professional_id` (FK)
    * `DimEducations`: `education_id` (PK), `professional_id` (FK)
    * `FactJobs`: `job_id` (PK), `professional_id` (FK)

### 4. Data Pipeline Approach and Assumptions :

* **Approach:**
    * Use Python (Pandas) for data transformation, validation, and cleaning.
    * Use DuckDB for data storage and querying.
    * Create a dimensional model to support analytical queries.
* **Assumptions:**
    * The JSON data has a consistent structure.
    * Date formats are parseable.
    * Professional ID, Job ID, skill ID, certification ID, and education ID are unique.
    * Null values in end\_date means that the job is the current job.
* **Incremental Loading Considerations:**
    * For incremental loading, we could add a "last\_updated" timestamp to each professional record in the source JSON.
    * During extraction, we would query a metadata table (or similar) to get the timestamp of the last successful load.
    * We would then filter the source data to only include records where "last\_updated" is greater than the last load timestamp.
    * Alternatively, if the source system provides it, a change data capture (CDC) mechanism would be ideal.
    * **Note: The provided DAG does not implement incremental loading.**

## Part 2: Requirements Gathering & Problem Space Analysis

### 1. Specific Questions for Customer Support Team:

* "What are the most common career transition patterns you observe (e.g., industry changes, role promotions)?"
* "Which industries and roles are currently in high demand among job seekers?"
* "What are the key factors that contribute to successful career progression within specific industries?"

### 2. Key Metrics:

* **Average Job Tenure (Job Duration):**
    * Valuable for understanding how long professionals stay in roles and companies.
* **Skill Acquisition Rate:**
    * Valuable for understanding how often professionals acquire new skills and how skills change over time.
* **Industry Transition Frequency:**
    * How often do professionals change industries. Helps to understand career flexibility.

### 3. Data Model Support:

* **Average Job Tenure:**
    * The `FactJobs` table stores job start and end dates, allowing for easy calculation of job duration.
* **Skill Acquisition Rate:**
    * The `DimSkills` table and `FactJobs` table can be used to track skill changes over time.
* **Industry Transition Frequency:**
    * The `FactJobs` table contains the industry field, which can be used to see how often professionals transition between industries.

### 4. Additional Data Sources:

* **Job Postings:**
    * To understand current job market demands and skill requirements.
* **Salary Surveys:**
    * To analyze salary trends and industry benchmarks.


## Part 3: Airflow Integration & Production Considerations

### 1. Airflow DAG Design:

* **DAG ID:** `Career_Data_Pipeline`
* **Schedule:** `@once`
* **Tasks:**
    * `extract_data`: Extracts data from `professionals_nested.json`.
    * `transform_data`: Transforms data from previous step to create professionals, jobs, skills, certifications, and education dataframes.
    * `validate_data`: Perform validation for format and logical inconsistencies.
    * `load_data`: Loads the transformed data into DuckDB.
* **Task Dependencies:** `extract_data >> transform_data >> validate_data >> load_data`

* **Incremental Data Loading:**
    * As mentioned above, incremental loading is possible but is **not** implemented in this DAG.

* **DAG Sepcifications:**
    * The specs are stored in .env file and used as paramter in docker compose.

### 2. Production Considerations:

* **Monitoring and Alerting:**
    * Implement Airflow monitoring and alerting to track DAG runs and task failures.
* **Error Handling:**
    * Add robust error handling to all tasks to gracefully handle exceptions.
* **Data Backup and Recovery:**
    * Implement a backup and recovery plan for the DuckDB database to prevent data loss.
* **Security:**
    * Securely store database credentials and other sensitive information.
* **Scalability:**
    * Use a cloud database or distributed system if data volume increases.
* **Incremental Loading Strategy:**
    * Implement an incremental loading strategy as described in Part 1 to reduce processing time and resource consumption.
* **Idempotency:**
    * Ensure tasks are idempotent to prevent data duplication or inconsistencies in case of retries.
* **Schema Drift:**   
    * Validate the incoming JSON data against a defined schema. Alert on schema changes that require manual intervention