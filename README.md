# airflow_
This is in partial fulfillment of my data engineering certificate with TDI

## AIRFLOW BATCH PROCESSING WORKFLOW 
This specific assignment addresses  my understanding of ETL with the batch process to smoothen client interaction with data. By using a request API, data is moved from a given user platform to interact with the [Spoonacular API](https://spoonacular.com/food-api) and collect a list of recipes by every specific user. The project utilizes 3 major steps in the workflow to fulfill the following requirements;

<sub>***1. Handle user ingredient submissions and submit the unaltered data to the S3 buckets raw folder. User ingredient submissions will be repeated and we want to avoid recommitting these to the raw database. Each user submission should be batched together in collections of 20 by Airflow and sent in batch responses to the raw folder.***</sub> 

<sub>***2. A sensor should monitor submissions to the raw folder and then utilize the Spoonacular API to collect recipes based on their submissions. All this data should be maintained and transferred to the stage folder within the S3 bucket.***</sub>

<sub>***3. Another sensor should monitor submissions to the stage folder and then organize this information appropriately into a PostgreSQL relational database.***</sub>

### Step 1: Read user data from API and save it to the bucket
This step involves creating two functions, where the first one requests the user data from the [API](https://airflow-miniproject.onrender.com), and the second one uploads the data to an [AWS](https://aws.amazon.com/console/) s3 bucket folder named userdata/. The request API reads the data in batches of 20 and returns a list of 20 nested dictionaries including pantry and user information. This data is uploaded as is into the s3 bucket. 

`
		@task(dag=dag)
		def request_api_callback():
			...
`

`
		@task(dag=dag)
		def upload_to_bucket():
		    ...
`

### Step 2: Read user data from API and save it to the bucket
