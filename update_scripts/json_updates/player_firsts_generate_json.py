import json
import psycopg2
import firebase_admin
from firebase_admin import credentials, firestore
from sub.stat_combo_clean_for_json import clean_combo

# Load the credentials from the JSON file
with open('update_scripts/json_updates/sub/duncan-nba-website-firebase-adminsdk-fbsvc-20bb82a9cc.json') as cred_file:
    firebase_credentials = json.load(cred_file)

cred = credentials.Certificate(firebase_credentials)
firebase_admin.initialize_app(cred)
db = firestore.client()

team_name_map = {
    "SAC": "Sacramento Kings",
    "GSW": "Golden State Warriors",
    "LAL": "Los Angeles Lakers",
    "LAC": "Los Angeles Clippers",
    "PHO": "Phoenix Suns",
    "DAL": "Dallas Mavericks",
    "HOU": "Houston Rockets",
    "SAS": "San Antonio Spurs",
    "MEM": "Memphis Grizzlies",
    "NOP": "New Orleans Pelicans",
    "DEN": "Denver Nuggets",
    "OKC": "Oklahoma City Thunder",
    "POR": "Portland Trailblazers",
    "MIN": "Minnesota Timberwolves",
    "UTA": "Utah Jazz",
    "ATL": "Atlanta Hawks",
    "BOS": "Boston Celtics",
    "BKN": "Brooklyn Nets",
    "CHA": "Charlotte Hornets",
    "CHI": "Chicago Bulls",
    "CLE": "Cleveland Cavaliers",
    "DET": "Detroit Pistons",
    "IND": "Indiana Pacers",
    "MIA": "Miami Heat",
    "MIL": "Milwaukee Bucks",
    "NYK": "New York Knicks",
    "ORL": "Orlando Magic",
    "PHI": "Philadelphia 76ers",
    "TOR": "Toronto Raptors",
    "WAS": "Washington Wizards"
}

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

cur.execute("SELECT player_id, player_name, date, team_id, stats, game_type FROM player_stat_firsts ORDER BY date DESC LIMIT 3;")
rows = cur.fetchall()

player_stat_firsts = [{"player_id": row[0], 
         "player_name": row[1],
         "date": row[2].isoformat(),
         "team_id": row[3],
         "stats": row[4],
         "team_name": team_name_map.get(row[3], "Unknown Team"),
         "game_type": row[5]} for row in rows]

for player in player_stat_firsts:
    print(player)
    player["text"] = clean_combo(player)

#print(json.dumps(player_stat_firsts, indent=4))

collection_ref = db.collection("player_stat_firsts")

# Step 1: Delete existing documents in the collection
docs = collection_ref.stream()
deleted_count = 0
for doc in docs:
    doc.reference.delete()
    deleted_count += 1

#Iterate through the records and upload them to Firestore
for player in player_stat_firsts:
    # Create a unique document ID using player_id and date
    doc_id = f"{player['player_id']}_{player['date']}"
    doc_ref = collection_ref.document(doc_id)

    # Check if the document already exists
    if not doc_ref.get().exists:
        # If it doesn't exist, add the document
        doc_ref.set(player)
        print(f"Added {player['player_name']} on {player['date']}")
    else:
        # If it exists, skip adding it
        print(f"Skipping duplicate: {player['player_name']} on {player['date']}")

print("Data successfully uploaded to Firestore!")

cur.close()
conn.close()