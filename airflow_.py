import os
import glob
import json
import requests
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.s3_key_sensor import S3KeySensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(minutes=5),
    'catchup': True
}

#1- Read user data from API and save to the bucket
def request_api_callback():
    responses = []
    for _ in range(20):
        response = requests.get('https://airflow-miniproject.onrender.com')
        user_ingredients = response.json()  # process request
        responses.append(user_ingredients)
    return responses  # returns a list of 20 dictionaries

def upload_to_bucket():
    s3_hook = S3Hook(aws_conn_id='conn_iam')
    bucket_name = 'insert_name'
    folder_path = 'userdata/'
    stage_folder = 'apidata/'
    file_name = f'20_user-{datetime.now()}.json'
    s3_key = folder_path + file_name
     
    user_ingredients_list = request_api_callback()
    json_data = json.dumps(user_ingredients_list)
        
    s3_hook.load_string(
        string_data=json_data,
        key=s3_key,
        bucket_name=bucket_name,
    )
#2- Process the data in the s3 bucket from raw folder to stage_folder
#here I am modifying the script to return all results to every id within the user.
#Putting all results in one file
def process_and_save_s3_data(**kwargs):
    s3_hook = S3Hook(aws_conn_id='add_conn')  # Specify your AWS connection ID
    bucket_name = 'insert_name'  # S3 bucket name
    folder_path = 'userdata/'  # folder path
    stage_folder = 'apidata/'  # stage folder path
    all_user_recipe_data = []  # List to store all user recipe data

    try:
        keys = s3_hook.list_keys(bucket_name, prefix=folder_path)
        for key in keys:
            obj_data = s3_hook.read_key(key, bucket_name)
            if obj_data is not None:
                try:
                    data = json.loads(obj_data)
                    for item in data:
                        if 'pantry' in item and 'user' in item:
                            pantry_dict = item['pantry']
                            user_dict = item['user']

                            url = 'https://api.spoonacular.com/recipes/complexSearch'
                            params = {
                                'apiKey': api_key,
                                'addRecipeInformation': 'true',
                                'includeNutrition': 'true',
                                'includeRecipeInformation': 'true',
                                'instructionsRequired': 'true',
                                'fillIngredients': 'true',
                                'addRecipeNutrition': 'true',
                                #'addRecipeEquipment': 'true'  # Include recipe equipment
                            }
                            params.update(pantry_dict)

                            sp_response = requests.get(url, params=params)

                            if sp_response.status_code == 200:
                                api_response = sp_response.json()
                                recipes = api_response['results']
                                user_recipe_data = {
                                    'name': user_dict['name'],
                                    'address': user_dict['address'],
                                    'description': user_dict['description'],
                                    'recipes': []
                                }
                                for recipe in recipes:
                                    recipe_data = {
                                        'title': recipe['title'],
                                        'servings': recipe['servings'],
                                        'summary': recipe['summary'],
                                        'diets': recipe['diets'],
                                        'ingredients': recipe['extendedIngredients']
                                    }

                                    user_recipe_data['recipes'].append(recipe_data)

                                all_user_recipe_data.append(user_recipe_data)
                            else:
                                print(f"Request failed with status code: {sp_response.status_code}")
                except Exception as e:
                    print(f"Error processing data: {str(e)}")
            else:
                print(f"Failed to retrieve data from S3 for key: {key}")

        # Store all user recipe data in a single file
        all_user_recipe_json = json.dumps(all_user_recipe_data)
        stage_key = stage_folder + 'all_user_recipes.json'
        s3_hook.load_string(
            all_user_recipe_json,
            stage_key,
            bucket_name=bucket_name,
            replace=True
        )

    except Exception as e:
        print(f"Error reading data from S3: {str(e)}")

#3 - create database and load data
def create_tables(**kwargs):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname='add_name',
        user='add_name',
        password='sjUIX4AO2__Pms2KHNVrOlGWhuXCuYCj',
        port='5432',
        host='stompd.db.elephantsql.com'
    )
    cursor = conn.cursor()

    try:
        # Create the tables
        cursor.execute('CREATE TABLE IF NOT EXISTS users (name VARCHAR, address VARCHAR, description VARCHAR);')
        cursor.execute('CREATE TABLE IF NOT EXISTS recipes (title VARCHAR, servings INTEGER, summary VARCHAR, diets JSON, ingredients JSON);')
        cursor.execute('CREATE TABLE IF NOT EXISTS user_recipes (user_name VARCHAR, recipe_title VARCHAR);')

        conn.commit()
        print("Tables created successfully.")

    finally:
        # Close the database connection
        if conn is not None:
            cursor.close()
            conn.close()



def upload_to_postgres(bucket_name, stage_folder, **kwargs):
    s3_hook = S3Hook(aws_conn_id='add_conn')  # Specify your AWS connection ID

    # Fetch data (list of file keys)
    objects = s3_hook.list_keys(bucket_name=bucket_name, prefix=stage_folder)

    # Check if any files are found
    if not objects:
        raise ValueError("No files found in the specified S3 folder.")

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname='add_name',
        user='add_name',
        password='sjUIX4AO2__Pms2KHNVrOlGWhuXCuYCj',
        port='5432',
        host='stompd.db.elephantsql.com'
    )

    try:
        # Create a new cursor and execute SQL statements
        cursor = conn.cursor()

        try:
            # Process each file
            for file_key in objects:
                # Read the JSON file from S3
                json_data = s3_hook.read_key(file_key, bucket_name=bucket_name)

                # Parse the JSON data
                try:
                    json_objects = json.loads(json_data)
                    print("JSON data loaded successfully.")

                    # Iterate over the JSON objects and insert into PostgreSQL
                    for obj in json_objects:
                        user_name = obj['name']
                        address = obj['address']
                        description = obj['description']

                        # Check if the user already exists
                        cursor.execute("SELECT name FROM users WHERE name = %s", (user_name,))
                        existing_user = cursor.fetchone()

                        if not existing_user:
                            # Insert into PostgreSQL table
                            cursor.execute("INSERT INTO users (name, address, description) VALUES (%s, %s, %s)",
                                           (user_name, address, description))

                        if 'recipes' in obj:
                            recipes = obj['recipes']
                            for recipe in recipes:
                                title = recipe['title']
                                servings = recipe['servings']
                                summary = recipe['summary']
                                diets = json.dumps(recipe['diets'])

                                # Check if the recipe already exists
                                cursor.execute("SELECT title FROM recipes WHERE title = %s", (title,))
                                existing_recipe = cursor.fetchone()

                                if not existing_recipe:
                                    # Extract and format ingredients
                                    ingredients_list = []
                                    ingredients = recipe['ingredients']
                                    for ingredient in ingredients:
                                        ingredient_id = ingredient['id']
                                        aisle = ingredient['aisle']
                                        image = ingredient['image']
                                        consistency = ingredient['consistency']
                                        ingredient_name = ingredient['name']
                                        name_clean = ingredient['nameClean']
                                        original = ingredient['original']
                                        original_name = ingredient['originalName']
                                        amount = ingredient['amount']
                                        unit = ingredient['unit']
                                        meta = json.dumps(ingredient['meta'])
                                        measures = json.dumps(ingredient['measures'])

                                        ingredient_entry = {
                                            'id': ingredient_id,
                                            'aisle': aisle,
                                            'image': image,
                                            'consistency': consistency,
                                            'name': ingredient_name,
                                            'name_clean': name_clean,
                                            'original': original,
                                            'original_name': original_name,
                                            'amount': amount,
                                            'unit': unit,
                                            'meta': meta,
                                            'measures': measures
                                        }

                                        ingredients_list.append(json.dumps(ingredient_entry))

                                    # Convert the list of ingredients to JSON array
                                    ingredients_array = json.dumps(ingredients_list)

                                    # Insert into PostgreSQL table
                                    cursor.execute("INSERT INTO recipes (title, servings, summary, diets, ingredients) VALUES (%s, %s, %s, %s, %s)",
                                                   (title, servings, summary, diets, ingredients_array))

                                # Insert into user_recipes table
                                cursor.execute("INSERT INTO user_recipes (user_name, recipe_title) VALUES (%s, %s)",
                                               (user_name, title))

                    conn.commit()

                except json.JSONDecodeError as e:
                    print(f"Invalid JSON data in file '{file_key}': {e}")

        finally:
            # Close the cursor
            if cursor is not None:
                cursor.close()

    finally:
        # Close the database connection
        if conn is not None:
            conn.close()


with DAG(dag_id='data_s3_db', schedule_interval= None, default_args=default_args) as dag:
    api_key = 'e50bf762dc0d495d942437a2498cb898'  # Replace with your actual Spoonacular API key
    bucket_name = 'insert_name'
    folder_path = 'userdata/'
    stage_folder = 'apidata/'
    
    request_api_task = PythonOperator(
        task_id='request_api',
        python_callable=request_api_callback
    )
    upload_to_bucket_task = PythonOperator(
        task_id="upload_to_bucket",
        python_callable=upload_to_bucket
    )

    s3_key_sensor = S3KeySensor(
        task_id='s3_key_sensor',
        bucket_key=f'{folder_path}',
        wildcard_match=True,
        bucket_name=bucket_name,
        timeout=60 * 60 * 24,  # 1 day timeout
        poke_interval=60,  # Check every minute
    )

    process_s3_task = PythonOperator(
        task_id='process_s3_data',
        python_callable=process_and_save_s3_data,
        op_kwargs={'bucket_name': bucket_name, 'stage_folder': 'apidata/'},
        provide_context=True,
    )
    
    s3_sensor = S3KeySensor(
        task_id='s3_sensor',
        bucket_key=f'{folder_path}',
        wildcard_match=True,
        bucket_name=bucket_name,
        timeout=60 * 60 * 24,  # 1 day timeout
        poke_interval=60,  # Check every minute
    )
    
    create_tables_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables,
        provide_context=True
    )

    upload_task = PythonOperator(
        task_id='upload_to_postgres',
        python_callable=upload_to_postgres,
        op_kwargs={
            'bucket_name': 'insert_name',
            'stage_folder': 'apidata/'
        },
        provide_context=True
    )

request_api_task >> upload_to_bucket_task >> s3_key_sensor >> process_s3_task >> s3_sensor >> create_tables_task >> upload_task