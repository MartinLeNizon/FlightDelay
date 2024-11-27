import datetime
import random
import json
import requests
from faker import Faker
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_batch

# Initialize Faker for generating random names
fake = Faker()

# ============================
# DAG Default Arguments
# ============================
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'start_date': datetime.datetime(2024, 10, 1),  # Start date of the DAG
    'retries': 1,  # Number of retries in case of failure
    'retry_delay': datetime.timedelta(minutes=5),  # Delay between retries
}

# ============================
# DAG Definition
# ============================
dag = DAG(
    dag_id='dnd_character_generator',  # Unique identifier for the DAG
    default_args=default_args,  # Default arguments
    schedule_interval='0 12 * * 5',  # Schedule: Every Friday at 12:00 PM
    catchup=False,  # Do not perform backfill
    description='Generates D&D characters weekly and stores them in Postgres',  # Description of the DAG
)

# ============================
# Task 1: Start Dummy Operator
# ============================
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# ============================
# Task 2: Generate Characters
# ============================
def generate_characters(**kwargs):
    """
    Generates basic character information: name, race, class, and level.
    Fetches races and classes dynamically from the D&D API.
    """
    characters = []  # List to hold character dictionaries

    # Fetch races from the D&D API
    races_response = requests.get("https://www.dnd5eapi.co/api/races")
    if races_response.status_code != 200:
        raise Exception(f"Failed to fetch races: {races_response.status_code}")
    races_data = races_response.json().get('results', [])
    races = [race['index'] for race in races_data]  # Extract race indices

    # Fetch classes from the D&D API
    classes_response = requests.get("https://www.dnd5eapi.co/api/classes")
    if classes_response.status_code != 200:
        raise Exception(f"Failed to fetch classes: {classes_response.status_code}")
    classes_data = classes_response.json().get('results', [])
    classes = [cls['index'] for cls in classes_data]  # Extract class indices

    for _ in range(5):  # Generate 5 characters
        character = {
            'name': fake.unique.first_name(),  # Generate a unique first name
            'race': random.choice(races),  # Randomly select a race from API data
            'class': random.choice(classes),  # Randomly select a class from API data
            'level': random.randint(1, 3),  # Assign a random level between 1 and 3
            'attributes': [],  # Placeholder for attributes
            'languages': '',    # Placeholder for languages
            'proficiency_choices': '',  # Placeholder for proficiencies
            'spells': '',       # Placeholder for spells
        }
        characters.append(character)  # Add the character to the list

    print(f"Generated characters: {characters}")  # Log generated characters

    # Push characters list to XCom for downstream tasks
    kwargs['ti'].xcom_push(key='characters', value=characters)

generate_characters_task = PythonOperator(
    task_id='generate_characters',
    python_callable=generate_characters,  # Function to execute
    dag=dag,
)

# ============================
# Task 3: Generate Attributes
# ============================
def generate_attributes(**kwargs):
    """
    Assigns random attributes to each character.
    Attributes include: strength, dexterity, constitution, intelligence, wisdom, charisma.
    Each attribute is assigned a random integer within specified ranges.
    """
    # Pull the characters list from XCom
    characters = kwargs['ti'].xcom_pull(key='characters', task_ids='generate_characters')

    for character in characters:
        # Assign random attributes within specified ranges
        attributes = [
            random.randint(6, 18),  # Strength: 6-18
            random.randint(2, 18),  # Dexterity: 2-18
            random.randint(2, 18),  # Constitution: 2-18
            random.randint(2, 18),  # Intelligence: 2-18
            random.randint(2, 18),  # Wisdom: 2-18
            random.randint(2, 18),  # Charisma: 2-18
        ]
        character['attributes'] = attributes  # Assign attributes to the character

    print(f"Assigned attributes: {characters}")  # Log assigned attributes

    # Push updated characters back to XCom
    kwargs['ti'].xcom_push(key='characters', value=characters)

generate_attributes_task = PythonOperator(
    task_id='generate_attributes',
    python_callable=generate_attributes,  # Function to execute
    dag=dag,
)

# ============================
# Task 4: Assign Proficiencies and Languages
# ============================
def assign_proficiencies_languages(**kwargs):
    """
    Assigns proficiencies and languages to each character using their indices from the D&D API.
    Proficiencies: Assign 2 random proficiencies.
    Languages: Assign 1 random language.
    """
    # Pull the characters list from XCom
    characters = kwargs['ti'].xcom_pull(key='characters', task_ids='generate_attributes')

    # Fetch proficiencies from the D&D API
    prof_response = requests.get("https://www.dnd5eapi.co/api/proficiencies")
    if prof_response.status_code != 200:
        raise Exception(f"Failed to fetch proficiencies: {prof_response.status_code}")
    prof_data = prof_response.json().get('results', [])  # Get the list of proficiencies

    # Fetch languages from the D&D API
    lang_response = requests.get("https://www.dnd5eapi.co/api/languages")
    if lang_response.status_code != 200:
        raise Exception(f"Failed to fetch languages: {lang_response.status_code}")
    lang_data = lang_response.json().get('results', [])  # Get the list of languages

    # Create mappings from lowercased, spaceless names to indices
    prof_map = {prof['name'].replace(' ', '').lower(): prof['index'] for prof in prof_data}
    lang_map = {lang['name'].replace(' ', '').lower(): lang['index'] for lang in lang_data}

    prof_keys = list(prof_map.keys())  # List of proficiency keys
    lang_keys = list(lang_map.keys())  # List of language keys

    for character in characters:
        # Assign 2 random proficiencies
        try:
            selected_profs = random.sample(prof_keys, 2)  # Select 2 unique proficiencies
            prof_indices = [prof_map[prof] for prof in selected_profs]  # Get their indices
            character['proficiency_choices'] = ",".join(prof_indices)  # Join indices into a comma-separated string
        except Exception as e:
            print(f"Error assigning proficiencies for {character['name']}: {e}")
            character['proficiency_choices'] = ""  # Assign empty string in case of error

        # Assign 1 random language
        try:
            selected_language = random.choice(lang_keys)  # Select 1 language
            lang_index = lang_map[selected_language]  # Get its index
            character['languages'] = lang_index  # Assign the language index
        except Exception as e:
            print(f"Error assigning language for {character['name']}: {e}")
            character['languages'] = ""  # Assign empty string in case of error

    print(f"Assigned proficiencies and languages with indices: {characters}")  # Log assignments

    # Push updated characters back to XCom
    kwargs['ti'].xcom_push(key='characters', value=characters)

assign_proficiencies_languages_task = PythonOperator(
    task_id='assign_proficiencies_languages',
    python_callable=assign_proficiencies_languages,  # Function to execute
    dag=dag,
)

# ============================
# Task 5: Branch to Assign Spells
# ============================
def fetch_spellcasting_classes():
    """
    Fetches the list of spellcasting classes from the D&D 5e API.
    Returns a list of class indices that can cast spells.
    """
    response = requests.get("https://www.dnd5eapi.co/api/classes")  # Fetch all classes
    if response.status_code != 200:
        raise Exception(f"Failed to fetch classes: {response.status_code}")

    classes_data = response.json().get('results', [])  # Get the list of classes
    spellcasting_classes = []  # List to hold spellcasting classes

    for cls in classes_data:
        # Fetch detailed information for each class
        class_response = requests.get(f"https://www.dnd5eapi.co{cls['url']}")
        if class_response.status_code != 200:
            print(f"Warning: Failed to fetch details for class {cls['index']}")
            continue  # Skip this class if details cannot be fetched
        class_detail = class_response.json()
        if 'spellcasting' in class_detail:
            # If the class has spellcasting abilities, add it to the list
            spellcasting_classes.append(cls['index'])  # Use class index as identifier

    print(f"Spellcasting classes: {spellcasting_classes}")  # Log spellcasting classes
    return spellcasting_classes

def branch_assign_spells(**kwargs):
    """
    Determines whether to assign spells based on the classes of the characters.
    If any character is a spellcasting class, proceed to assign spells.
    Otherwise, skip to collecting characters.
    """
    # Pull the characters list from XCom
    characters = kwargs['ti'].xcom_pull(key='characters', task_ids='assign_proficiencies_languages')

    # Dynamically fetch spellcasting classes
    spellcasting_classes = fetch_spellcasting_classes()

    # Check if any character has a spellcasting class
    can_cast_spells = any(char['class'] in spellcasting_classes for char in characters)
    print(f"Can cast spells: {can_cast_spells}")  # Log the branching decision

    if can_cast_spells:
        return 'assign_spells'  # Proceed to assign spells
    else:
        return 'collect_characters'  # Skip assigning spells

branch_assign_spells_task = BranchPythonOperator(
    task_id='branch_assign_spells',
    python_callable=branch_assign_spells,  # Function to execute
    dag=dag,
)

# ============================
# Task 6: Assign Spells
# ============================
def assign_spells(**kwargs):
    """
    Assigns spells to characters that can cast them using indices from the D&D API.
    """
    # Pull the characters list from XCom
    characters = kwargs['ti'].xcom_pull(key='characters', task_ids='assign_proficiencies_languages')

    # Dynamically fetch spellcasting classes
    spellcasting_classes = fetch_spellcasting_classes()

    # Fetch all spells from the D&D API
    response = requests.get("https://www.dnd5eapi.co/api/spells")
    if response.status_code != 200:
        raise Exception(f"Failed to fetch spells: {response.status_code}")
    spells_data = response.json().get('results', [])  # Get the list of spells

    # Initialize a dictionary to hold spells for each spellcasting class
    class_spells = {cls: [] for cls in spellcasting_classes}

    # Populate the class_spells dictionary with spell indices
    for spell in spells_data:
        # Fetch detailed information for each spell
        spell_response = requests.get(f"https://www.dnd5eapi.co{spell['url']}")
        if spell_response.status_code == 200:
            spell_detail = spell_response.json()
            spell_level = spell_detail.get('level', 0)  # Get spell level
            if spell_level > 2:
                continue  # Skip spells above level 2
            for cls in spellcasting_classes:
                # Get list of classes that can cast the spell
                classes = [c['index'] for c in spell_detail.get('classes', [])]
                if cls in classes:
                    class_spells[cls].append(spell['index'])  # Add spell index to the class
        else:
            print(f"Warning: Failed to fetch details for spell {spell['index']}")  # Log any failures

    # Assign spells to each character
    for character in characters:
        if character['class'] in spellcasting_classes:
            level = character['level']  # Get character's level
            num_spells = level + 3  # Number of spells to assign: level + 3
            available_spells = class_spells.get(character['class'], [])  # Get available spells for the class
            if available_spells:
                # Select random spells, ensuring not to exceed available spells
                selected_spells = random.sample(available_spells, min(num_spells, len(available_spells)))
                character['spells'] = ",".join(selected_spells)  # Assign spells as comma-separated indices
            else:
                character['spells'] = ""  # Assign empty string if no spells are available
        else:
            character['spells'] = ""  # Assign empty string for non-spellcasting classes

    print(f"Assigned spells: {characters}")  # Log assigned spells

    # Push updated characters back to XCom
    kwargs['ti'].xcom_push(key='characters', value=characters)

assign_spells_task = PythonOperator(
    task_id='assign_spells',
    python_callable=assign_spells,  # Function to execute
    dag=dag,
)

# ============================
# Task 7: Collect Characters
# ============================
def collect_characters(**kwargs):
    """
    Collects the final character data after assigning spells (if applicable).
    """
    # Attempt to pull characters from assign_spells_task
    characters = kwargs['ti'].xcom_pull(key='characters', task_ids='assign_spells')

    if characters:
        print("Collected characters after assigning spells.")  # Log collection after spells
    else:
        # If assign_spells was skipped, pull characters from assign_proficiencies_languages_task
        characters = kwargs['ti'].xcom_pull(key='characters', task_ids='assign_proficiencies_languages')
        print("Collected characters without assigning spells.")  # Log collection without spells

    print(f"Final characters: {characters}")  # Log final characters

    # Push characters to XCom for insertion
    kwargs['ti'].xcom_push(key='characters', value=characters)

collect_characters_task = PythonOperator(
    task_id='collect_characters',
    python_callable=collect_characters,  # Function to execute
    dag=dag,
)

# ============================
# Task 8: Insert into PostgreSQL
# ============================
def insert_into_postgres_callable(**kwargs):
    """
    Inserts the final character data into the PostgreSQL database using bulk insert.
    """
    # Initialize PostgresHook with the connection ID
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    # Pull the final characters from collect_characters_task
    characters = kwargs['ti'].xcom_pull(key='characters', task_ids='collect_characters')

    if not characters:
        print("No characters to insert.")  # Log if there are no characters
        return  # Exit the function

    # Define the SQL insert query with placeholders
    insert_query = """
        INSERT INTO public.dnd_characters (
            name, attributes, race, languages, class, proficiency_choices, level, spells
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    records = []  # List to hold tuples of character data
    for char in characters:
        records.append((
            char['name'],                     # Name of the character
            json.dumps(char['attributes']),    # Convert attributes list to JSON string
            char['race'],                     # Race index of the character
            char['languages'],                # Language index
            char['class'],                    # Class index of the character
            char['proficiency_choices'],      # Proficiencies indices as comma-separated string
            char['level'],                    # Level of the character
            char['spells'],                   # Spells indices as comma-separated string
        ))

    print(f"Inserting {len(records)} characters into Postgres.")  # Log number of records
    print(f"Records: {records}")  # Log the actual records being inserted

    try:
        # Get a connection to Postgres
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        # Execute the bulk insert
        execute_batch(cursor, insert_query, records)
        # Commit the transaction
        conn.commit()
        print(f"Inserted {len(records)} characters into Postgres.")  # Confirm insertion
    except Exception as e:
        raise Exception(f"Failed to insert characters into Postgres: {str(e)}")  # Raise exception on failure

insert_into_postgres = PythonOperator(
    task_id='insert_into_postgres',
    python_callable=insert_into_postgres_callable,  # Function to execute
    dag=dag,
)

# ============================
# Task 9: End Dummy Operator
# ============================
end = DummyOperator(
    task_id='end',
    trigger_rule='none_failed',  # Proceed only if all upstream tasks did not fail
    dag=dag,
)

# ============================
# DAG Task Dependencies
# ============================
start >> generate_characters_task \
      >> generate_attributes_task \
      >> assign_proficiencies_languages_task \
      >> branch_assign_spells_task

# Branching: If assign_spells_task is chosen, proceed accordingly
branch_assign_spells_task >> assign_spells_task \
                        >> collect_characters_task \
                        >> insert_into_postgres \
                        >> end

# If branch is to skip assign_spells_task, proceed directly to collect_characters_task
branch_assign_spells_task >> collect_characters_task \
                        >> insert_into_postgres \
                        >> end

# ============================
# End of DAG
#⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀   ⠀⠀⠀⠀⠀⠀⢰⣤⡀⠀⠀⠀⠀⠀⠀
#⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀   ⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⣿⣤⣤⣄⠀⠀⠀
#⠀⠀⠀⠀⠀⠀⠀⠀⠀   ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣾⣿⣿⣿⣿⣿⣷⣶⣶
#⠀⠀⠀⠀⠀⠀   ⠀⠀⠀⠀⠀⣀⣤⣴⣶⣶⣶⣶⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠟
#⠀⠀⠀   ⠀⣀⠀⠀⠀⣠⣶⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠛⠛⠛⠁⠀
#⠀⠀⠀   ⢰⣿⣷⣦⣼⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡟⠁⠀⠀⠀⠀⠀
#   ⠀⠀⠀⠀⠉⠻⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠏⠀⠀⠀⠀⠀⠀⠀
#⠀⠀⠀⠀   ⠀⠀⢸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠋⠀⠀⠀⠀⠀⠀⠀⠀
#⠀   ⠀⠀⠀⠀⠀⢸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀
#⠀   ⠀⠀⠀⠀⠀⠈⠻⣿⣿⣿⣿⣿⡇⠀⠀⢿⣿⣿⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
#⠀   ⠀⢀⡄⠀⠀⠀⠀⠈⠻⣿⣿⣿⠇⠀⠀⢸⣿⣿⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
#⠀   ⣀⢼⣿⣦⡄⠀⠀⠀⢰⣿⣿⠏⠀⠀⠀⠈⣿⣿⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
#   ⠼⠿⠿⠿⠿⠿⠆⠀⠀⠿⠿⠏⠀⠀⠀⠀⠀⠹⠿⠿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
# ============================
