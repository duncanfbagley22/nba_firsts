import psycopg2
import random
from decimal import Decimal

# Database connection
#database_user = input("Enter Database Username: ").strip().lower()
#database_password = input("Enter Database Password: ").strip().lower()

def connect_to_db():
    return psycopg2.connect(
        dbname="nba_data",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )


def get_game_type():
    connection = connect_to_db()
    cursor = connection.cursor()
    query_game_type = """
            SELECT distinct game_type
            FROM staging_player_stats
        """
    cursor.execute(query_game_type)
    result = cursor.fetchone()[0]

    return result
    

# Define stat configurations
STAT_CONFIG = [
    {"stat": "team_id", "relevance": 3, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": []},
    {"stat": "mp", "relevance": 1, "type": "less_than", "rounding": "up", "rounding_value": 5, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": []},
    {"stat": "fg", "relevance": 1, "type": "greater_than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": []},
    {"stat": "fga", "relevance": 2, "type": "less_than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["efg_pct", "ts_pct"]},
    {"stat": "fg_pct", "relevance": 1, "type": "greater_than", "rounding": "down", "rounding_value": .05, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["efg_pct", "ts_pct"]},
    {"stat": "fg3", "relevance": 1, "type": "greater_than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": []},
    {"stat": "fg3a", "relevance": 2, "type": "less_than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["efg_pct", "ts_pct"]},
    {"stat": "fg3_pct", "relevance": 1, "type": "greater_than", "rounding": "down", "rounding_value": .05, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["efg_pct", "ts_pct"]},
    {"stat": "ft", "relevance": 1, "type": "greater_than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": []},
    {"stat": "fta", "relevance": 2, "type": "less_than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["ts_pct"]},
    {"stat": "ft_pct", "relevance": 1, "type": "greater_than", "rounding": "down", "rounding_value": .05, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["ts_pct", "efg_pct"]},
    {"stat": "orb", "relevance": 1, "type": "greater_than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["drb", "trb"]},
    {"stat": "drb", "relevance": 1, "type": "greater_than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["orb", "trb"]},
    {"stat": "trb", "relevance": 1, "type": "greater_than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["drb", "orb"]},
    {"stat": "ast", "relevance": 1, "type": "greater_than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": []},
    {"stat": "stl", "relevance": 1, "type": "greater_than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": []},
    {"stat": "blk", "relevance": 1, "type": "greater_than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": []},
    {"stat": "tov", "relevance": 1, "type": "less_than", "rounding": "false", "rounding_value": 1, "allow_zeros": "true", "only_true": "false", "mutually_exclusive": []},
    {"stat": "pf", "relevance": 1, "type": "less_than", "rounding": "false", "rounding_value": 1, "allow_zeros": "true", "only_true": "false", "mutually_exclusive": []},
    {"stat": "pts", "relevance": 1, "type": "greater_than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": []},
    {"stat": "first_name", "relevance": 4, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["last_name"]},
    {"stat": "last_name", "relevance": 4, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["first_name"]},
    {"stat": "height", "relevance": 2, "type": "greater than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["weight", "height"]},
    {"stat": "height", "relevance": 2, "type": "less than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["weight", "height"]}, 
    {"stat": "weight", "relevance": 3, "type": "greater than", "rounding": "down", "rounding_value": 5, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["weight", "height"]},
    {"stat": "weight", "relevance": 3, "type": "less than", "rounding": "up", "rounding_value": 5, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["weight", "height"]},
    {"stat": "birth_month", "relevance": 3, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["birth_date", "birth_year", "birth_city", "birth_state", "birth_country"]},
    {"stat": "birth_date", "relevance": 4, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["birth_month", "birth_year", "birth_city", "birth_state", "birth_country"]},
    {"stat": "birth_year", "relevance": 3, "type": "greater than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["birth_date", "birth_month", "birth_year", "birth_city", "birth_state", "birth_country"]},
    {"stat": "birth_year", "relevance": 3, "type": "less than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["birth_date", "birth_month", "birth_year", "birth_city", "birth_state", "birth_country"]},
    #{"stat": "birth_city", "relevance": 4, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["birth_country", "birth_state", "birth_month"]},
    {"stat": "birth_state", "relevance": 3, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["birth_country", "birth_city", "birth_month", "birth_date", "birth_year"]},
    {"stat": "birth_country", "relevance": 3, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["birth_state", "birth_city", "birth_month", "birth_date", "birth_year"]},
    {"stat": "draft_round", "relevance": 2, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "true", "only_true": "false", "mutually_exclusive": ["draft_year"]},
    {"stat": "draft_year", "relevance": 3, "type": "greater than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["draft_round", "draft_year"]},
    {"stat": "draft_year", "relevance": 3, "type": "less than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["draft_round", "draft_year"]},
    {"stat": "dominant_hand", "relevance": 2, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": []},
    {"stat": "game_month", "relevance": 2, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["game_date", "season"]},
    {"stat": "game_date", "relevance": 4, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["game_month", "season"]},
    {"stat": "game_day_of_week", "relevance": 3, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": []},
    {"stat": "overtime", "relevance": 2, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": []},
    {"stat": "venue", "relevance": 4, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["game_location"]},
    {"stat": "game_location", "relevance": 3, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["venue"]},
    {"stat": "season", "relevance": 2, "type": "greater than", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["game_month", "game_date"]},
    {"stat": "player_home_away", "relevance": 2, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": []},
    {"stat": "player_win_lose", "relevance": 2, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": []},
    {"stat": "age_at_time_of_game", "relevance": 2, "type": "greater than", "rounding": "down", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["age_at_time_of_game"]},
    {"stat": "age_at_time_of_game", "relevance": 2, "type": "less than", "rounding": "up", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["age_at_time_of_game"]},
    {"stat": "double_double", "relevance": 2, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "true", "mutually_exclusive": ["triple_double"]},
    {"stat": "triple_double", "relevance": 2, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "true", "mutually_exclusive": ["double_double"]},
    {"stat": "ts_pct", "relevance": 1, "type": "greater_than", "rounding": "down", "rounding_value": .05, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["fg_pct", "fg3_pct", "efg_pct", "ft_pct", "fga", "fg3a", "fta"]},
    {"stat": "efg_pct", "relevance": 1, "type": "greater_than", "rounding": "down", "rounding_value": .05, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": ["fg_pct", "fg3_pct", "ts_pct", "fga", "fg3a", "ft_pct"]},
    #{"stat": "game_type", "relevance": 1, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": []},
    {"stat": "opp_team_id", "relevance": 2, "type": "unique", "rounding": "false", "rounding_value": 1, "allow_zeros": "false", "only_true": "false", "mutually_exclusive": []}
]

def process_stats(name, id, data, game_type, cur):

    # Helper function to parse a stat string
    def parse_stat(stat):
        for op in ['>=', '<=', '>', '<', '=']:  # Ordered by precedence to handle multi-character operators first
            if op in stat:
                stat_name, value = stat.split(op, 1)
                return stat_name.strip(), op, value.strip()
        return None, None, None  # Return None if no operator is found (unlikely)

    # Parse all stats
    parsed_stats = [parse_stat(stat) for stat in data]

    query = f"""
            SELECT MAX(date) 
            FROM staging_games
        """
    cur.execute(query)
    query_date = cur.fetchall()

    if isinstance(id, set):
        id_for_query = next(iter(id))  # Get the first item from the set
    else:
        id_for_query = id  # Handle cases where it's already a string

    id_for_query = id_for_query.strip("'")  # Remove any surrounding quotes
    id_for_query = id_for_query.strip("{}")  # Remove braces if they exist

    # Use parameterized query to avoid SQL injection
    query = """
            SELECT game_id
            FROM staging_player_boxscores
            WHERE player_id = %s
        """
    cur.execute(query, (id_for_query,))
    query_game_id = cur.fetchall()
    
    # Create structured output
    return {
        "id": id,
        "name": name,
        "date": query_date,
        "game_id": query_game_id,
        "stats": [{"stat_name": stat[0], "operator": stat[1], "value": stat[2]} for stat in parsed_stats],
        "game_type": game_type
    }

# Weighted random selection based on relevance
def select_stats(data, sample_size):

  relevance_weights = {
      4: 0.01,
      3: 0.05,
      2: 0.20,
      1: 0.74
  }

  # Calculate weights based on relevance
  weights = [relevance_weights.get(item['relevance'], 0) for item in data]


  sample = []
  excluded_stats = set()
  selected_indices = set()

  # Select indices without duplicates
  while len(sample) < sample_size:
    index = random.choices(range(len(data)), weights, k=1)[0]
    
    # Skip if already selected
    if index in selected_indices:
        continue
    
    stat = data[index]


# Skip if the stat is mutually exclusive with already selected ones
    if stat['stat'] in excluded_stats:
        continue
    
    sample.append(stat)
    selected_indices.add(index)
    excluded_stats.update(stat['mutually_exclusive'])

  return sample

def remove_conditions_iteratively(player_id, cur, conditions, game_type):
    while True:
        condition_removed = False  # Track if we remove any condition in this pass
        for condition in conditions:
            # Create a temporary set of conditions without the current one
            temp_conditions = [c for c in conditions if c != condition]
            if len(temp_conditions) == 1:
                temp_conditions = temp_conditions[0]
                query = f"""
                    SELECT COUNT(*) 
                    FROM (
                        SELECT *
                        FROM player_stats
                        WHERE {temp_conditions}
                        UNION ALL
                        SELECT *
                        FROM staging_player_stats
                        WHERE {temp_conditions}
                        AND player_id <> '{player_id}'
                        AND game_type = '{game_type}'             
                    ) AS combined
                """
            elif len(temp_conditions) > 1:
                query = f"""
                    SELECT COUNT(*) 
                    FROM (
                        SELECT *
                        FROM player_stats
                        WHERE {' AND '.join(temp_conditions)}
                        UNION ALL
                        SELECT *
                        FROM staging_player_stats
                        WHERE {' AND '.join(temp_conditions)}
                        AND player_id <> '{player_id}'  
                        AND game_type = '{game_type}'           
                    ) AS combined
                """
            cur.execute(query)
            count = cur.fetchone()[0]
            # If removing this condition still results in count == 0
            if count == 0:
                #print(f"Removing condition: {condition}")
                conditions = temp_conditions  # Update the conditions
                condition_removed = True  # Mark that we made a change
                break  # Restart the loop with the updated conditions
        if not condition_removed:
            break  # Exit the loop if no conditions can be removed
    return conditions


# Main function
def evaluate_staging_data():
    conn = connect_to_db()
    cur = conn.cursor()
    game_type = get_game_type()
    try:
        # Fetch staging data
        cur.execute("""
            select * from staging_player_stats
        """)
        staging_data = cur.fetchall()

        # Fetch historical data
        cur.execute("""
            select * from player_stats
        """)
        historical_data = cur.fetchall()


        field_names = [
            '_id', 'game_id', 'team_id', 'player_id', 'mp', 'fg', 'fga', 'fg_pct', 'fg3', 'fg3a',
            'fg3_pct', 'ft', 'fta', 'ft_pct', 'orb', 'drb', 'trb', 'ast', 'stl', 'blk',
            'tov', 'pf', 'pts', 'first_name', 'last_name', 'height', 'weight', 'birth_month', 'birth_date',
            'birth_year', 'birth_city', 'birth_state', 'birth_country', 'draft_round', 'draft_year', 'dominant_hand', 'game_month',
            'game_date', 'game_day_of_week', 'overtime', 'venue', 'game_location', 'season', 'player_home_away', 'player_win_lose', 'age_at_time_of_game',
            'double_double', 'triple_double', 'ts_pct', 'efg_pct', 'game_type', 'opp_team_id'
        ]

        staging_data = [
            dict(zip(field_names, row)) for row in staging_data
        ]
        # Loop through players in random order
        random.shuffle(staging_data)
        
        Y = 5  # Number of stats per combination
        
        Y_max = 8
        X_increment = 0
        while Y<=Y_max:
            counter = 0

            X = 10 + X_increment # Number of unique combinations per player
            #print("Current number of stats per combination:", Y)
            while counter < X:
                stats_to_check = select_stats(STAT_CONFIG, Y)
                stats_to_check_clean = [item['stat'] for item in stats_to_check]
                #print("Stats being checked: ", stats_to_check_clean)

                for record in staging_data:
                    player_name = record["first_name"] + " " + record["last_name"]
                    player_id = record["player_id"]
                    #print(f"Evaluating player: {player_name} (ID: {player_id})")
                    being_checked = [item["stat"] for item in stats_to_check]
                    #print(being_checked)
                    conditions = []
                    
                    for stat in stats_to_check:
                        if stat['allow_zeros'] == 'false' and record[stat["stat"]] == 0:
                            break  # Skip the rest of the stats for this record
                        if stat['only_true'] == 'true' and record[stat["stat"]] == 'No':
                            break
                        if stat['stat'] == 'mp' and record[stat["stat"]] < 5:
                            break
                        # Create a unique condition for each combination
                        stat_name = stat['stat']
                        rounded_value = record[stat['stat']]
                        rounding = stat.get('rounding', 'false')
                        rounding_value = Decimal(str(stat.get('rounding_value', 1)))
                        
                        
                        # Apply rounding logic if rounding is not false
                        if rounding != 'false':
                            stat_value = Decimal(str(record[stat_name]))
                            if rounding == 'down':
                                rounded_value = (stat_value // rounding_value) * rounding_value
                            elif rounding == 'up':
                                rounded_value = (stat_value // rounding_value) * rounding_value + rounding_value
                        else:
                            rounded_value = record[stat['stat']]  # No rounding applied

                        if stat["type"] == "greater_than":
                            conditions.append(f"{stat['stat']} >= {rounded_value}")
                        elif stat["type"] == "less_than":
                            conditions.append(f"{stat['stat']} <= {rounded_value}")
                        elif stat_name == "draft_round":
                            conditions.append(f"{stat['stat']} = {record[stat['stat']]}")
                        elif stat["type"] == "unique":
                            conditions.append(f"{stat['stat']} = '{record[stat['stat']]}'")
                
                    count = 1
                    if len(conditions) == Y:

                        query = f"""
                                SELECT COUNT(*) 
                                FROM (
                                    SELECT *
                                    FROM player_stats
                                    WHERE {' AND '.join(conditions)}
                                    and game_type = '{game_type}'  
                                    UNION ALL
                                    SELECT *
                                    FROM staging_player_stats
                                    WHERE {' AND '.join(conditions)}
                                    and player_id <> '{player_id}'  
                                    and game_type = '{game_type}'           
                                    )
                            """
                        cur.execute(query)
                        count = cur.fetchone()[0]
                    
                    if count == 0:

                            # Create a relevance lookup from STAT_CONFIG
                            relevance_lookup = {item["stat"]: item["relevance"] for item in STAT_CONFIG}

                            # Function to extract stat from condition
                            def extract_stat(condition):
                                return condition.split(' ')[0]

                            # Sort conditions based on relevance
                            conditions = sorted(
                                conditions,
                                key=lambda cond: relevance_lookup.get(extract_stat(cond), float('inf')),  # Use infinity for stats not in STAT_CONFIG
                                reverse=True
                            )

                            updated_result = process_stats({player_name},{player_id}, remove_conditions_iteratively(player_id, cur, conditions, game_type), game_type, cur)
                            print(updated_result)

                            return updated_result # Exit the script on the first unique combination
                
                counter += 1
            Y +=1
            X_increment +=2

    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

# Run the script
if __name__ == "__main__":
    evaluate_staging_data()