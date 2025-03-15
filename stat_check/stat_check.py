import sub.unique_combo_one as unique_combo_one, sub.stat_combo_clean as stat_combo_clean
import json
import psycopg2
import time

def connect_to_db():
    return psycopg2.connect(
        dbname="nba_data",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

def record_exists(conn, unique_id):
    """Check if a record with the given unique_id already exists in the database."""
    query = """
        SELECT EXISTS(
            SELECT 1 
            FROM player_stat_firsts 
            WHERE _id = %s
        );
    """
    with conn.cursor() as cur:
        cur.execute(query, (unique_id,))
        return cur.fetchone()[0]

def insert_player_stat_instance(data):
    # Extract data from the input dictionary
    player_id = next(iter(data["id"]))  # Extract the single value from the set
    player_name = next(iter(data["name"]))  # Extract the single value from the set
    date = data["date"][0][0]  # Extract the date value
    game_id = data["game_id"][0][0]  # Extract the game ID
    stats = json.dumps(data["stats"])  # Convert stats to JSON string
    unique_id = f"{player_id}_{date}"  # Create the unique_id
    game_type = data["game_type"]
    team_id = data["team_id"]
    
    conn = connect_to_db()
    
    try:
        if record_exists(conn, unique_id):
            print(f"A record for player_id '{player_id}' on date '{date}' already exists.")
            overwrite = input("Do you want to overwrite it? (y/n): ").strip().lower()
            if overwrite != 'y':
                print("Operation canceled.")
                return
        with conn.cursor() as cur:
            query = """
                INSERT INTO player_stat_firsts (_id, player_id, player_name, date, game_id, stats, game_type, team_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (_id)
                DO UPDATE SET
                    player_name = EXCLUDED.player_name,
                    game_id = EXCLUDED.game_id,
                    stats = EXCLUDED.stats;
            """
            cur.execute(query, (unique_id, player_id, player_name, date, game_id, stats, game_type, team_id))
            conn.commit()
            print("Record inserted/updated successfully.")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        raise

    finally:
        conn.close()

def process_instances(num_instances, timeout=180):
    processed_ids = set()

    for instance_num in range(1, num_instances + 1):
        print(f"Instance #{instance_num}: ")
        
        start_time = time.time()
        # Generate a unique combination result
        while True:
            combination_result = unique_combo_one.evaluate_staging_data()
            stat_combo_clean.clean_combo(combination_result)
            
            # Extract the player ID
            player_id = next(iter(combination_result["id"]))
            
            # Check if the ID is already processed
            if player_id not in processed_ids:
                processed_ids.add(player_id)
                break
            else:
                print(f"Duplicate ID '{player_id}' detected. Generating a new combination...")
            elapsed_time = time.time() - start_time
            if elapsed_time > timeout:
                print("Timeout reached. Unable to find a unique combination.")
                return  # Exit the function or break the loop if needed

        approval = input("Do you want to insert this record into the database? (y/n): ").strip().lower()
        
        if approval == 'y':
            insert_player_stat_instance(combination_result)
        else:
            print("Record not added.")



process_instances(3, timeout=180)