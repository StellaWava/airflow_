#a demonstration of improvements that can be made to the code. This files still hangs on execution 

import os
import json
import requests
import psycopg2
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.s3_key_sensor import S3KeySensor


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 9),  # Replace with an appropriate start date
    'catchup': True
}

dag = DAG(
    dag_id='data_s3_then_db',
    schedule_interval=None,
    default_args=default_args
)


# Define connection details as constants
POSTGRES_DB = 'add_name'
POSTGRES_USER = 'add_name'
POSTGRES_PASSWORD = 'sjUIX4AO2__Pvs2KHNVrOlGWhuXCuYCj'
POSTGRES_PORT = '5432'
POSTGRES_HOST = 'stampdv.db.elephantsql.com'
SPOONACULAR_API_KEY = 'e50bf762dc0d495d942437a2498cb892'
BUCKET_NAME = 'insert_name'
FOLDER_PATH = 'userdata/'
STAGE_FOLDER = 'apidata/'



@task (dag=dag)
def request_api_callback():
    responses = []
    for _ in range(20):
        response = requests.get('https://airflow-miniproject.onrender.com') #instructor link
        user_ingredients = response.json()  # process request
        responses.append(user_ingredients)
    return responses  # returns a list of 20 dictionaries


@task (dag=dag)
def upload_to_bucket():
    s3_hook = S3Hook(aws_conn_id='aws_conn')
    file_name = f'20_user-{datetime.now()}.json'
    s3_key = FOLDER_PATH + file_name
     
    user_ingredients_list = request_api_callback()
    json_data = json.dumps(user_ingredients_list)
        
    s3_hook.load_string(
        string_data=json_data,
        key=s3_key,
        bucket_name=BUCKET_NAME,
    )


@task (dag=dag)
def process_and_save_s3_data():
    s3_hook = S3Hook(aws_conn_id='aws_conn')
    all_user_recipe_data = []

    try:
        keys = s3_hook.list_keys(BUCKET_NAME, prefix=FOLDER_PATH)
        for key in keys:
            obj_data = s3_hook.read_key(key, BUCKET_NAME)
            if obj_data is not None:
                try:
                    data = json.loads(obj_data)
                    for item in data:
                        if 'pantry' in item and 'user' in item:
                            pantry_dict = item['pantry']
                            user_dict = item['user']

                            url = 'https://api.spoonacular.com/recipes/complexSearch'
                            params = {
                                'apiKey': 'e50bf762dc0d495d942437a2498cb898',
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
        stage_key = STAGE_FOLDER + 'all_user_recipes.json'
        s3_hook.load_string(
            all_user_recipe_json,
            stage_key,
            bucket_name=BUCKET_NAME,
            replace=True
        )

    except Exception as e:
        print(f"Error reading data from S3: {str(e)}")


@task (dag=dag)
def create_tables():
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT,
        host=POSTGRES_HOST
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


@task (dag =dag)
def upload_to_postgres():
    s3_hook = S3Hook(aws_conn_id='aws_conn')
    objects = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=STAGE_FOLDER)

    if not objects:
        raise ValueError("No files found in the specified S3 folder.")

    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT,
        host=POSTGRES_HOST
    )

    try:
        cursor = conn.cursor()

        try:
            for file_key in objects:
                json_data = s3_hook.read_key(file_key, bucket_name=BUCKET_NAME)

                try:
                    json_objects = json.loads(json_data)
                    print("JSON data loaded successfully.")

                    for obj in json_objects:
                        name = obj['name']
                        address = obj['address']
                        description = obj['description']

                        cursor.execute("SELECT name FROM users WHERE name = %s", (name,))
                        existing_user = cursor.fetchone()

                        if not existing_user:
                            cursor.execute("INSERT INTO users (name, address, description) VALUES (%s, %s, %s)",
                                           (name, address, description))

                        if 'recipes' in obj:
                            recipes = obj['recipes']
                            for recipe in recipes:
                                title = recipe['title']
                                servings = recipe['servings']
                                summary = recipe['summary']
                                diets = json.dumps(recipe['diets'])

                                cursor.execute("SELECT title FROM recipes WHERE title = %s", (title,))
                                existing_recipe = cursor.fetchone()

                                if not existing_recipe:
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

                                    ingredients_array = json.dumps(ingredients_list)

                                    cursor.execute("INSERT INTO recipes (title, servings, summary, diets, ingredients) VALUES (%s, %s, %s, %s, %s)",
                                                   (title, servings, summary, diets, ingredients_array))

                                cursor.execute("INSERT INTO user_recipes (user_name, recipe_title) VALUES (%s, %s)",
                                               (user_name, title))

                    conn.commit()

                except json.JSONDecodeError as e:
                    print(f"Invalid JSON data in file '{file_key}': {e}")

        finally:
            if cursor is not None:
                cursor.close()

    finally:
        if conn is not None:
            conn.close()


dag = DAG(
    dag_id='data_s3_then_db',
    schedule_interval=None,
    default_args=default_args
)

# Define task dependencies
request_api_task = request_api_callback()
upload_to_bucket_task = upload_to_bucket()
s3_key_sensor = S3KeySensor(
    task_id='s3_key_sensor',
    bucket_key=FOLDER_PATH,
    wildcard_match=True,
    bucket_name=BUCKET_NAME,
    timeout=60 * 60 * 24,  # 1 day timeout
    poke_interval=60,  # Check every minute
)
process_s3_task = process_and_save_s3_data()
create_tables_task = create_tables()
upload_task = upload_to_postgres()


# Set task dependencies
request_api_task >> upload_to_bucket_task >> process_s3_task >> create_tables_task >> upload_task
