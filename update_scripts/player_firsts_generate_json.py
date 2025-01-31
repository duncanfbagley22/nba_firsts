import json
import psycopg2

def connect_to_database():
    return psycopg2.connect(
        dbname="nba_data",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

conn = connect_to_database()

cur = conn.cursor()

cur.execute("SELECT player_id, player_name, date, team_id, stats FROM player_stat_firsts ORDER BY date DESC LIMIT 3;")
rows = cur.fetchall()

player_stat_firsts = [{"player_id": row[0], 
         "player_name": row[1],
         "date": row[2].isoformat(),
         "team_id": row[3],
         "stats": row[4]} for row in rows]

print(json.dumps(player_stat_firsts, indent=4))

# with open("player_stat_firsts.json", "w") as json_file:
#     json.dump(player_stat_firsts, json_file, indent=4)

cur.close()
conn.close()