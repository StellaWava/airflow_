# airflow_
This is in partial fulfillment of my data engineering certificate with TDI

## AIRFLOW BATCH PROCESSING WORKFLOW 
This assignment aims to enhance my comprehension of ETL (Extract, Transform, Load) processes, specifically focusing on batch processing to enhance client interactions with data. The primary objective is to leverage a request API to extract data from a user platform and facilitate interactions with the [Spoonacular API](https://spoonacular.com/food-api) in order to gather a personalized list of recipes for each user. To accomplish this, the project follows a workflow consisting of three key steps. Each step plays a crucial role in meeting the specified requirements;

<sub>***1. Handle user ingredient submissions and submit the unaltered data to the S3 buckets raw folder. User ingredient submissions will be repeated and we want to avoid recommitting these to the raw database. Each user submission should be batched together in collections of 20 by Airflow and sent in batch responses to the raw folder.***</sub> 

<sub>***2. A sensor should monitor submissions to the raw folder and then utilize the Spoonacular API to collect recipes based on their submissions. All this data should be maintained and transferred to the stage folder within the S3 bucket.***</sub>

<sub>***3. Another sensor should monitor submissions to the stage folder and then organize this information appropriately into a PostgreSQL relational database.***</sub>

### Step 1: Read user data from API and save it to the bucket
In this step, we focus on the creation of two essential functions. The first function is responsible for initiating a request to the designated [API](https://airflow-miniproject.onrender.com) to retrieve user data. The second function handles the subsequent task of uploading the acquired data to a designated folder named "userdata/" within an [AWS](https://aws.amazon.com/console/) S3 bucket.

To extract the user data, the request API is designed to process the information in batches of 20. Each batch consists of a list of 20 nested dictionaries, encompassing both pantry and user-related details. This data, without any modifications, is then directly uploaded to the specified S3 bucket for storage and future use. 

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

### Step 2: Process the data in the s3 bucket from the userdata folder to stage_folder
In this step, a crucial function is implemented to collect data from the S3 bucket folder. The collected data, in the form of a JSON object, is then used to extract significant recipe information from the Spoonacular API. The focus is on retrieving recipe details that are specifically relevant to each user, utilizing the pantry and user information available within each file of the JSON object.

For every file in the object obtained from the S3 bucket, the function extracts the pantry and user information. This information is then submitted to the Spoonacular API, enabling the retrieval of all recipes associated with that particular user. It's important to note that the number of recipe requests can vary greatly among users, with some users having numerous requests while others may have none.

Once the recipe data is processed, it is stored in the designated stage S3 bucket folder called "apidata/" as a single object file. This folder serves as a repository for the organized and processed recipe information, facilitating further analysis or utilization in subsequent stages of the workflow.

		`
		@task(dag=dag)
		def process_and_save_s3_data():
    			...
		`



### Step 3: create database and load data from the stage folder
This final step of the workflow involves the implementation of two functions. The first function is responsible for creating tables in the database, while the second function facilitates the transfer of data from the S3 bucket folder into these tables. Three distinct tables are created: "users," "recipes," and "user_recipes."

The "users" table captures comprehensive user information, including the user's name, address, and description. The "recipes" table stores all relevant recipe details. Lastly, the "user_recipes" table establishes the connection between user records and recipe records, containing the user's name and the respective recipe's information.

It is important to note that this project relies on an airflow model/setup that does not have a PostgreSQL connection enabled. As an alternative, ElephantSQL is utilized as the database. The functions ensure the creation of the necessary tables and facilitate the movement of data from the S3 bucket folder to the respective tables in the ElephantSQL database.

By completing this final step, the workflow successfully establishes a structured database environment and ensures the availability of organized user and recipe data, with the appropriate connections established between the two entities.

		`
		@task(dag=dag)
		def create_tables():
		    ...
		`

		`
		@task(dag=dag)
		def upload_to_postgres():
		    ...
		`

Lastly, to enable seamless interaction between the functions, an S3 sensor is employed to connect all the aforementioned steps. This sensor ensures that the workflow progresses smoothly by monitoring the availability of data in the S3 bucket before triggering the execution of subsequent tasks.

In Apache Airflow, a [DAG (Directed Acyclic Graph)](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html) is created to combine and orchestrate the seven tasks involved in this workflow. Each task represents a specific function plus the s3 sensors, and the DAG ensures its sequential execution. By utilizing Apache Airflow's scheduling capabilities, the DAG can be configured to run at regular intervals.

However, considering the limitations imposed by the API requests, the DAG schedule is set to None for this project. This means that the DAG does not run automatically based on a predefined schedule. Instead, it requires manual triggering to initiate the execution.

By combining the power of Apache Airflow, the S3 sensor, and the DAG, this workflow ensures a systematic and efficient execution of the ETL process, facilitating the collection, transformation, and loading of data from user platforms to the database while adhering to the limitations of the API requests.


#### Notes
1. All credentials are removed.
2. There is room for improving the code. This is a demonstration of my understanding of batch processing with Apache Airflow. 

						#### THANK YOU








