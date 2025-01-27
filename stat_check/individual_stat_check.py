import psycopg2
from psycopg2.extras import RealDictCursor


database_user = input("Enter Database Username: ").strip().lower()
database_password = input("Enter Database Password: ").strip().lower()
# Database connection details
db_config = {
    'dbname': 'nba_data',
    'user': database_user,
    'password': database_password,
    'host': 'localhost',
    'port': '5432',
}

# Stats to compare
STATS = ["fg", "fga", "fg3", "fg3a", "ft", "fta", "orb", "drb", "trb", "ast", "stl", "blk", "tov", "pts"]

# Years for recent top 10 analysis
RECENT_YEARS = 10

def connect_db():
    try:
        conn = psycopg2.connect(**db_config)
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def get_season_year(game_id, cursor):
    cursor.execute("SELECT season FROM games WHERE game_id = %s", (game_id,))
    result = cursor.fetchone()
    return result['season'] if result else None

def analyze_stats():
    conn = connect_db()
    if not conn:
        return

    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Fetch staging data
        cursor.execute("SELECT * FROM staging_player_boxscores")
        staging_data = cursor.fetchall()

        results = {"first_occurrences": [], "top_10": [], "recent_top_10": []}

        for record in staging_data:
            player_id = record['player_id']
            game_id = record['game_id']
            season_year = get_season_year(game_id, cursor)

            for stat in STATS:
                stat_value = record[stat]

                # Skip if the stat value is None
                if stat_value is None:
                    continue

                # Check if it's a first occurrence
                cursor.execute(f"SELECT COUNT(*) FROM player_boxscores WHERE {stat} = %s AND {stat} IS NOT NULL", (stat_value,))
                count = cursor.fetchone()["count"]
                if count == 0:
                    results["first_occurrences"].append(
                        f"Player {player_id} had the first game with {stat_value} {stat}."
                    )

                # Check if it's in the top 10 overall
                cursor.execute(f"SELECT {stat} FROM player_boxscores WHERE {stat} IS NOT NULL ORDER BY {stat} DESC LIMIT 10")
                top_10_values = [row[stat] for row in cursor.fetchall()]
                if top_10_values and stat_value >= min(top_10_values):
                    results["top_10"].append(
                        f"Player {player_id} had a game with {stat_value} {stat}, which is in the top 10."
                    )

                # Check if it's in the top 10 within the last RECENT_YEARS
                if season_year:
                    cursor.execute(
                        f"SELECT {stat} FROM player_boxscores pb JOIN games g ON pb.game_id = g.game_id "
                        f"WHERE g.season >= %s AND {stat} IS NOT NULL ORDER BY {stat} DESC LIMIT 10",
                        (season_year - RECENT_YEARS,)
                    )
                    recent_top_10_values = [row[stat] for row in cursor.fetchall()]
                    if recent_top_10_values and stat_value >= min(recent_top_10_values):
                        results["recent_top_10"].append(
                            f"Player {player_id} had a game with {stat_value} {stat}, which is in the top 10 for the last {RECENT_YEARS} years."
                        )


        # Output results
        print("--- First Occurrences ---")
        print("\n".join(results["first_occurrences"]))
        print("\n--- Top 10 Overall ---")
        print("\n".join(results["top_10"]))
        print("\n--- Top 10 in Last 10 Years ---")
        print("\n".join(results["recent_top_10"]))

    except Exception as e:
        print(f"Error during analysis: {e}")

    finally:
        conn.close()

if __name__ == "__main__":
    analyze_stats()
