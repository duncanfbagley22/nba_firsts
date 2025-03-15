import json
import psycopg2
from decimal import Decimal
from datetime import date
import requests
import base64

GITHUB_TOKEN = "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"
REPO_OWNER = "duncanfbagley22"
REPO_NAME = "nba-stat-website"
BRANCH = "main"


MOON_PHASE_EMOJIS = {
    "New Moon": "🌑",
    "Waxing Crescent": "🌒",
    "First Quarter": "🌓",
    "Waxing Gibbous": "🌔",
    "Full Moon": "🌕",
    "Waning Gibbous": "🌖",
    "Last Quarter": "🌗",
    "Waning Crescent": "🌘"
}

chart_one_query = """
with combined_result as (select REPLACE(ps.first_name, '.', '') as f_name, p.name as name, pts
from player_stats ps join players p on ps.player_id = p.player_id
where p.first_name like '_J' or p.first_name like '_.J_'

UNION ALL

select REPLACE(ps.first_name, '.', '') as f_name, p.name as name, pts
from staging_player_stats ps join players p on ps.player_id = p.player_id
where p.first_name like '_J' or p.first_name like '_.J_'),

f_name_totals as (select f_name, sum(pts) as group_total
from combined_result
group by 1)

select cr.f_name, 
cr.name as name, 
sum(cr.pts) as pts,
ROW_NUMBER() OVER (PARTITION BY cr.f_name ORDER BY sum(pts) DESC) AS rank,
fnt.group_total
from combined_result cr
join f_name_totals fnt on cr.f_name = fnt.f_name
group by 1, 2, 5
order by 5 desc, 3 desc
"""
chart_one_info = {
    "id": 1,
    "title": "Pull-Up J: Leading Scorers by Names Ending in J",
    "chartType": "stackedBar", 
}
chart_two_query = """
WITH combined_result AS (
    SELECT g.date, birth_state, pts
    FROM player_stats ps 
    JOIN games g ON ps.game_id = g.game_id
    WHERE birth_state IN ('Ohio', 'Pennsylvania', 'Michigan')

    UNION ALL

    SELECT g.date, birth_state, pts
    FROM staging_player_stats ps 
    JOIN staging_games g ON ps.game_id = g.game_id
    WHERE birth_state IN ('Ohio', 'Pennsylvania', 'Michigan')
),
final_combined_result AS (
    SELECT 
        cr.date, 
        cr.birth_state, 
        cr.pts,  -- Keep original points for reference
        SUM(cr.pts) OVER (PARTITION BY cr.birth_state ORDER BY cr.date ASC) AS cumulative_pts
    FROM combined_result cr
)

SELECT 
    EXTRACT(YEAR FROM fcr.date) AS year,  -- Extract the year from the date
    MAX(CASE WHEN fcr.birth_state = 'Ohio' THEN fcr.cumulative_pts ELSE NULL END) AS Ohio,
    MAX(CASE WHEN fcr.birth_state = 'Michigan' THEN fcr.cumulative_pts ELSE NULL END) AS Michigan,
    MAX(CASE WHEN fcr.birth_state = 'Pennsylvania' THEN fcr.cumulative_pts ELSE NULL END) AS Pennsylvania
FROM final_combined_result fcr
GROUP BY 
    EXTRACT(YEAR FROM fcr.date)
ORDER BY 
    year;
"""
chart_two_info = {
    "id": 2,
    "title": "The Rust Belt: Total Points Scored by Birth State Over Time",
    "chartType": "simpleLine", 
}
chart_three_query = """
SELECT
    team_id,
	phase as phase_text,
    SUM(CASE WHEN rank = 1 THEN 1 ELSE 0 END) AS total_wins,
    SUM(CASE WHEN rank = 2 THEN 1 ELSE 0 END) AS total_losses,
    SUM(CASE WHEN rank = 1 THEN 1 ELSE 0 END) + SUM(CASE WHEN rank = 2 THEN 1 ELSE 0 END) AS total_games,
	round(SUM(CASE WHEN rank = 1 THEN 1 ELSE 0 END)::NUMERIC / (SUM(CASE WHEN rank = 1 THEN 1 ELSE 0 END) + SUM(CASE WHEN rank = 2 THEN 1 ELSE 0 END))::NUMERIC, 3) as winning_percentage
FROM (
    -- Data from team_boxscores
    SELECT
		potm.phase,
        tbs.team_id, 
        tbs.pts,
        ROW_NUMBER() OVER (PARTITION BY tbs.game_id ORDER BY tbs.pts DESC) AS rank
    FROM team_boxscores tbs 
    JOIN games g ON tbs.game_id = g.game_id
	join phases_of_the_moon potm on g.date = potm.date
    
    UNION ALL
    
    -- Data from staging_games
    SELECT 
		potm.phase,
        tbs.team_id, 
        tbs.pts,
        ROW_NUMBER() OVER (PARTITION BY tbs.game_id ORDER BY tbs.pts DESC) AS rank
    FROM staging_team_boxscores tbs
    JOIN staging_games g ON tbs.game_id = g.game_id
	join phases_of_the_moon potm on g.date = potm.date
) combined_data
GROUP BY phase, team_id
ORDER BY team_id ASC,
    CASE phase
        WHEN 'New Moon' THEN 1 
        WHEN 'Waxing Crescent' THEN 2 
        WHEN 'First Quarter' THEN 3 
        WHEN 'Waxing Gibbous' THEN 4 
		WHEN 'Full Moon' THEN 5
        WHEN 'Waning Gibbous' THEN 6 
        WHEN 'Last Quarter' THEN 7
        WHEN 'Waning Crescent' THEN 8
        ELSE 9
    END;
"""
chart_three_info = {
    "id": 3,
    "title": "Shoot the Moon: Team Winning Percentage by Moon Phase",
    "chartType": "AreaChartFillByValue", 
}
chart_four_query = """
WITH combined AS (
    SELECT ps.player_id, COUNT(ps._id) AS count_of_id
    FROM player_stats ps 
    JOIN players p ON ps.player_id = p.player_id
    WHERE ps.pts IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97)
    GROUP BY ps.player_id

    UNION ALL

    SELECT ps.player_id, COUNT(ps._id)
    FROM staging_player_stats ps 
    JOIN players p ON ps.player_id = p.player_id
    WHERE ps.pts IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97)
    GROUP BY ps.player_id
)

SELECT 
    p.name,
    SUM(COUNT(ps._id)) OVER (PARTITION BY ps.player_id) AS group_total
FROM player_stats ps join players p on ps.player_id = p.player_id
WHERE ps.pts IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97)
AND ps.player_id IN (
    SELECT player_id
    FROM combined
    ORDER BY count_of_id DESC
    LIMIT 10
)
GROUP BY p.name, ps.player_id
ORDER BY group_total desc
"""
chart_four_info = {
    "id": 4,
    "title": "Prime Time: Players with the Most Career Games Scoring a Prime Number of Points",
    "chartType": "simpleBar", 
}
tracker_one_query = """
        WITH all_stats AS (
    SELECT * FROM player_stats WHERE triple_double = 'Yes'
    UNION ALL
    SELECT * FROM staging_player_stats WHERE triple_double = 'Yes'
),
triple_double_count AS (
    -- Count the number of triple-doubles for the season
    SELECT season, COUNT(*) AS triple_double_count
    FROM all_stats
    WHERE season = 2025
    GROUP BY season
),
latest_triple_double AS (
    SELECT DISTINCT ON (a.season) 
           a.season, 
           COALESCE(g.date, sg.date) AS max_game_date,  -- Take the available date
           a.player_id
    FROM all_stats a
    LEFT JOIN games g ON a.game_id = g.game_id
    LEFT JOIN staging_games sg ON a.game_id = sg.game_id
    WHERE a.season = 2025
    ORDER BY a.season, COALESCE(g.date, sg.date) DESC  -- Ensures we get the latest game first
)

SELECT tdc.season, 
       tdc.triple_double_count, 
       ltd.max_game_date, 
       ltd.player_id,
	   p.name
FROM triple_double_count tdc
JOIN latest_triple_double ltd ON tdc.season = ltd.season
JOIN players p on ltd.player_id = p.player_id;
"""
grid_one_query = """
    WITH all_games AS (
        SELECT 
            game_id,
            date,
            away_team_id AS team_id,
            away_team_name AS team_name,
            home_team_id AS opponent_id,
            home_team_name AS opponent_name,
            away_team_pts AS team_pts,
            home_team_pts AS opponent_pts,
            ot AS overtime,
            venue,
            location,
            game_type,
            season
        FROM games

        UNION ALL

        SELECT 
            game_id,
            date,
            home_team_id AS team_id,
            home_team_name AS team_name,
            away_team_id AS opponent_id,
            away_team_name AS opponent_name,
            home_team_pts AS team_pts,
            away_team_pts AS opponent_pts,
            ot AS overtime,
            venue,
            location,
            game_type,
            season
        FROM games
    )
    SELECT 
        team_id,
        MAX(CASE 
            WHEN team_id = 'ATL' THEN 9 
            WHEN opponent_id = 'ATL' THEN 1 
            ELSE 0 
        END) AS ATL,
        MAX(CASE 
            WHEN team_id = 'BKN' THEN 9 
            WHEN opponent_id = 'BKN' THEN 1 
            ELSE 0 
        END) AS BKN,
        MAX(CASE 
            WHEN team_id = 'BOS' THEN 9 
            WHEN opponent_id = 'BOS' THEN 1 
            ELSE 0 
        END) AS BOS,
        MAX(CASE 
            WHEN team_id = 'CHA' THEN 9 
            WHEN opponent_id = 'CHA' THEN 1 
            ELSE 0 
        END) AS CHA,
        MAX(CASE 
            WHEN team_id = 'CHI' THEN 9 
            WHEN opponent_id = 'CHI' THEN 1 
            ELSE 0 
        END) AS CHI,
        MAX(CASE 
            WHEN team_id = 'CLE' THEN 9 
            WHEN opponent_id = 'CLE' THEN 1 
            ELSE 0 
        END) AS CLE,
        MAX(CASE 
            WHEN team_id = 'DAL' THEN 9 
            WHEN opponent_id = 'DAL' THEN 1 
            ELSE 0 
        END) AS DAL,
        MAX(CASE 
            WHEN team_id = 'DEN' THEN 9 
            WHEN opponent_id = 'DEN' THEN 1 
            ELSE 0 
        END) AS DEN,
        MAX(CASE 
            WHEN team_id = 'DET' THEN 9 
            WHEN opponent_id = 'DET' THEN 1 
            ELSE 0 
        END) AS DET,
        MAX(CASE 
            WHEN team_id = 'GSW' THEN 9 
            WHEN opponent_id = 'GSW' THEN 1 
            ELSE 0 
        END) AS GSW,
        MAX(CASE 
            WHEN team_id = 'HOU' THEN 9 
            WHEN opponent_id = 'HOU' THEN 1 
            ELSE 0 
        END) AS HOU,
        MAX(CASE 
            WHEN team_id = 'IND' THEN 9 
            WHEN opponent_id = 'IND' THEN 1 
            ELSE 0 
        END) AS IND,
        MAX(CASE 
            WHEN team_id = 'LAC' THEN 9 
            WHEN opponent_id = 'LAC' THEN 1 
            ELSE 0 
        END) AS LAC,
        MAX(CASE 
            WHEN team_id = 'LAL' THEN 9 
            WHEN opponent_id = 'LAL' THEN 1 
            ELSE 0 
        END) AS LAL,
        MAX(CASE 
            WHEN team_id = 'MEM' THEN 9 
            WHEN opponent_id = 'MEM' THEN 1 
            ELSE 0 
        END) AS MEM,
        MAX(CASE 
            WHEN team_id = 'MIA' THEN 9 
            WHEN opponent_id = 'MIA' THEN 1 
            ELSE 0 
        END) AS MIA,
        MAX(CASE 
            WHEN team_id = 'MIL' THEN 9 
            WHEN opponent_id = 'MIL' THEN 1 
            ELSE 0 
        END) AS MIL,
        MAX(CASE 
            WHEN team_id = 'MIN' THEN 9 
            WHEN opponent_id = 'MIN' THEN 1 
            ELSE 0 
        END) AS MIN,
        MAX(CASE 
            WHEN team_id = 'NOP' THEN 9 
            WHEN opponent_id = 'NOP' THEN 1 
            ELSE 0 
        END) AS NOP,
        MAX(CASE 
            WHEN team_id = 'NYK' THEN 9 
            WHEN opponent_id = 'NYK' THEN 1 
            ELSE 0 
        END) AS NYK,
        MAX(CASE 
            WHEN team_id = 'OKC' THEN 9 
            WHEN opponent_id = 'OKC' THEN 1 
            ELSE 0 
        END) AS OKC,
        MAX(CASE 
            WHEN team_id = 'ORL' THEN 9 
            WHEN opponent_id = 'ORL' THEN 1 
            ELSE 0 
        END) AS ORL,
        MAX(CASE 
            WHEN team_id = 'PHI' THEN 9 
            WHEN opponent_id = 'PHI' THEN 1 
            ELSE 0 
        END) AS PHI,
        MAX(CASE 
            WHEN team_id = 'PHO' THEN 9 
            WHEN opponent_id = 'PHO' THEN 1 
            ELSE 0 
        END) AS PHO,
        MAX(CASE 
            WHEN team_id = 'POR' THEN 9 
            WHEN opponent_id = 'POR' THEN 1 
            ELSE 0 
        END) AS POR,
        MAX(CASE 
            WHEN team_id = 'SAC' THEN 9 
            WHEN opponent_id = 'SAC' THEN 1 
            ELSE 0 
        END) AS SAC,
        MAX(CASE 
            WHEN team_id = 'SAS' THEN 9 
            WHEN opponent_id = 'SAS' THEN 1 
            ELSE 0 
        END) AS SAS,
        MAX(CASE 
            WHEN team_id = 'TOR' THEN 9 
            WHEN opponent_id = 'TOR' THEN 1 
            ELSE 0 
        END) AS TOR,
        MAX(CASE 
            WHEN team_id = 'UTA' THEN 9 
            WHEN opponent_id = 'UTA' THEN 1 
            ELSE 0 
        END) AS UTA,
        MAX(CASE 
            WHEN team_id = 'WAS' THEN 9 
            WHEN opponent_id = 'WAS' THEN 1 
            ELSE 0 
        END) AS WAS
    FROM all_games
    WHERE overtime != 'REG'
    AND game_type = 'Regular Season'
    AND overtime != 'OT'
    GROUP BY team_id
    ORDER BY team_id;
"""

def get_file_sha(file_path):
    """Fetches the SHA of the existing file from GitHub (needed to update it)."""
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{file_path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()["sha"]  # Return SHA of existing file
    elif response.status_code == 404:
        return None  # File doesn't exist, will be created
    else:
        raise Exception(f"Error fetching SHA: {response.json()}")

def update_file_in_github(file_path, json_data):
    """Uploads the JSON data to GitHub, updating the file if it exists."""
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{file_path}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Get SHA if file exists
    sha = get_file_sha(file_path)
    
    # Convert JSON to Base64 (GitHub API requirement)
    json_content = json.dumps(json_data, indent=2)
    encoded_content = base64.b64encode(json_content.encode()).decode()

    payload = {
        "message": f"Updating {file_path}",
        "content": encoded_content,
        "branch": BRANCH
    }
    if sha:
        payload["sha"] = sha  # Required if updating an existing file

    response = requests.put(url, headers=headers, json=payload)

    if response.status_code in [200, 201]:
        print(f"Successfully updated {file_path} in GitHub.")
    else:
        raise Exception(f"GitHub API Error: {response.json()}")

def connect_to_database():
    return psycopg2.connect(
        dbname="nba_data",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )

def process_value(col, value):
    """ Process values based on column name. """
    if isinstance(value, Decimal):
        if col == "year":
            return str(value)  # Convert year to string
        elif col == "group_total":
            return int(value)  # Convert total_count to an integer
        elif col == "winning_percentage":
            return round(float(value), 3)  # Convert to float with 3 decimal places
    return value  # Default: return value as is

conn = connect_to_database()
cur = conn.cursor()

def generate_chart_json(query, info):

    cur.execute(query)
    col_names = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    data = []
    for row in rows:
        record = {col: process_value(col, value) for col, value in zip(col_names, row)}
        
        # If "phase" exists, add the corresponding moon emoji
        if "phase_text" in record:
            record["phase"] = MOON_PHASE_EMOJIS.get(record["phase_text"], "❓")  # Default to ❓ if not found
        
        data.append(record)#
    export_json = {
        "id": info["id"],
        "title": info["title"],
        "chartType": info["chartType"],
        "data": data
    }
    
    OUTPUT_FILE = f"json/chart{info['id']}.json"
    update_file_in_github(OUTPUT_FILE, export_json)

    print(f"Data exported to {OUTPUT_FILE} successfully!")

def generate_other_json(query, output_file_name):
    """Runs a SQL query and saves the result as a JSON file, handling dates & decimals."""
    
    cur.execute(query)
    col_names = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    # Process data: Convert Decimal to float and date to string
    data = [
        {col: (
            str(value) if isinstance(value, date)  # Convert date to "YYYY-MM-DD"
            else float(value) if isinstance(value, Decimal)  # Convert Decimal to float
            else value
        ) for col, value in zip(col_names, row)}
        for row in rows
    ]

    if output_file_name == "json/grid_one.json":

        
        # Convert all keys to uppercase, except "team_id"
        def convert_keys_to_upper(obj):
            if isinstance(obj, dict):
                return {k.upper() if k != "team_id" else k: convert_keys_to_upper(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_keys_to_upper(item) for item in obj]
            else:
                return obj

        data = convert_keys_to_upper(data)
        # If the output file is grid_one.json, structure data under "teams"
        data = {"teams": data}

    # Write the processed data to the output file
    update_file_in_github(output_file_name, data)  # Update file in GitHub
    
    print(f"Data exported to {output_file_name} successfully!")


generate_chart_json(chart_one_query, chart_one_info)
generate_chart_json(chart_two_query, chart_two_info)
generate_chart_json(chart_three_query, chart_three_info)
generate_chart_json(chart_four_query, chart_four_info)
generate_other_json(tracker_one_query, "json/tracker_one.json")
generate_other_json(grid_one_query, "json/grid_one.json")

cur.close()
conn.close()