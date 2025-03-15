import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
from time import sleep
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extras import execute_values
import pandas as pd
import re
import random
import numpy
import pycountry
import pprint

# DATABASE CONNECTION #

database_user = input("Enter Database Username: ").strip().lower()
database_password = input("Enter Database Password: ").strip().lower()

DB_CONFIG = {
    'dbname': 'nba_data',
    'user': database_user,
    'password': database_password,
    'host': 'localhost',
    'port': '5432',
}

db_connection = None

def get_connection():
    global db_connection
    if db_connection is None or db_connection.closed != 0:
        try:
            db_connection = psycopg2.connect(**DB_CONFIG)
            print("Database connection successful!")
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            raise
    return db_connection

def close_connection():
    global db_connection
    try:
        if db_connection and db_connection.closed == 0:
            db_connection.close()
            print("Database connection closed successfully!")
        else:
            print("No active database connection to close.")
    except Exception as e:
        print(f"Error closing the database connection: {e}")

# REFERENCE ARRAYS
TEAM_NAME_TO_ID = {
    "St. Louis Hawks": "ATL",
    "Milwaukee Hawks": "ATL",
    "Atlanta Hawks": "ATL",
    "Tri-Cities Blackhawks": "ATL",
    "Brooklyn Nets": "BKN",
    "New York Nets": "BKN",
    "New Jersey Nets": "BKN",
    "New Jersey Americans": "BKN",
    "Boston Celtics": "BOS",
    "Charlotte Hornets": "CHA",
    "Charlotte Bobcats": "CHA",
    "Chicago Bulls": "CHI",
    "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL",
    "Denver Nuggets": "DEN",
    "Denver Rockets": "DEN",
    "Detroit Pistons": "DET",
    "Fort Wayne Pistons": "DET",
    "San Francisco Warriors": "GSW",
    "Golden State Warriors": "GSW",
    "Philadelphia Warriors": "GSW",
    "San Diego Rockets": "HOU",
    "Houston Rockets": "HOU",
    "Indiana Pacers": "IND",
    "Buffalo Braves": "LAC",
    "San Diego Clippers": "LAC",
    "Los Angeles Clippers": "LAC",
    "Los Angeles Lakers": "LAL",
    "Minneapolis Lakers": "LAL",
    "Memphis Grizzlies": "MEM",
    "Vancouver Grizzlies": "MEM",
    "Miami Heat": "MIA",
    "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP",
    "NO/Ok. City Hornets": "NOP",
    "New Orleans Hornets": "NOP",
    "New Orleans/Oklahoma City Hornets": "NOP",
    "New York Knicks": "NYK",
    "Oklahoma City Thunder": "OKC",
    "Seattle SuperSonics": "OKC",
    "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI",
    "Syracuse Nationals": "PHI",
    "Phoenix Suns": "PHO",
    "Portland Trail Blazers": "POR",
    "Kansas City Kings": "SAC",
    "Rochester Royals": "SAC",
    "Cincinnati Royals": "SAC",
    "Kansas City-Omaha Kings": "SAC",
    "Sacramento Kings": "SAC",
    "Texas Chaparrals": "SAS",
    "Dallas Chaparrals": "SAS",
    "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR",
    "New Orleans Jazz": "UTA",
    "Utah Jazz": "UTA",
    "Capital Bullets": "WAS",
    "Chicago Zephyrs": "WAS",
    "Washington Bullets": "WAS",
    "Chicago Packers": "WAS",
    "Baltimore Bullets": "WAS",
    "Washington Wizards": "WAS"
}

VENUE_TO_LOCATION = {
    "STAPLES Center": "Los Angeles, CA",
    "Chase Center": "San Francisco, CA",
    "Oracle Arena": "Oakland, CA",
    "Ball Arena": "Denver, CO",
    "Pepsi Center": "Denver, CO",
    "Kia Center": "Orlando, FL",
    "Amway Center": "Orlando, FL",
    "Madison Square Garden (IV)": "New York City, NY",
    "TD Garden": "Boston, MA",
    "Little Caesars Arena": "Detroit, MI",
    "Spectrum Center": "Charlotte, NC",
    "Gainbridge Fieldhouse": "Indianapolis, IN",
    "Bankers Life Fieldhouse": "Indianapolis, IN",
    "Kaseya Center": "Miami, FL",
    "FTX Arena": "Miami, FL",
    "AmericanAirlines Arena": "Miami, FL",
    "Scotiabank Arena": "Toronto, ON",
    "Barclays Center": "Brooklyn, NY",
    "FedEx Forum": "Memphis, TN",
    "United Center": "Chicago, IL",
    "Delta Center": "Salt Lake City, UT",
    "Vivint Arena": "Salt Lake City, UT",
    "Vivint Smart Home Arena": "Salt Lake City, UT",
    "Frost Bank Center": "San Antonio, TX",
    "AT&T Center": "San Antonio, TX",
    "Crypto.com Arena": "Los Angeles, CA",
    "State Farm Arena": "Atlanta, GA",
    "Philips Arena": "Atlanta, GA",
    "Rocket Mortgage Fieldhouse": "Cleveland, OH",
    "Quicken Loans Arena": "Cleveland, OH",
    "American Airlines Center": "Dallas, TX",
    "Moda Center": "Portland, OR",
    "Golden 1 Center": "Sacramento, CA",
    "Capital One Arena": "Washington, DC",
    "Smoothie King Center": "New Orleans, LA",
    "Target Center": "Minneapolis, MN",
    "Footprint Center": "Phoenix, AZ",
    "Phoenix Suns Arena": "Phoenix, AZ",
    "Talking Stick Resort Arena": "Phoenix, AZ",
    "Paycom Center": "Oklahoma City, OK",
    "Chesapeake Energy Arena": "Oklahoma City, OK",
    "Toyota Center": "Houston, TX",
    "Fiserv Forum": "Milwaukee, WI",
    "Wells Fargo Center": "Philadelphia, PA",
    "Amalie Arena": "Tampa Bay, FL",
    "The Palace of Auburn Hills": "Detroit, MI",
    "Air Canada Centre": "Toronto, ON",
    "BMO Harris Bradley Center": "Milwaukee, WI",
    "Sleep Train Arena": "Sacramento, CA",
    "Verizon Center": "Washington, DC",
    "Time Warner Cable Arena": "Charlotte, NC",
    "The O2 Arena": "London, UK",
    "Mexico City Arena": "Mexico City, MX",
    "UW–Milwaukee Panther Arena": "Milwaukee, WI",
    "HP Field House": "Orlando, FL (Bubble)",
    "The Arena": "Orlando, FL (Bubble)",
    "Visa Athletic Center": "Orlando, FL (Bubble)",
    "US Airways Center": "Phoenix, AZ",
    "EnergySolutions Arena": "Salt Lake City, UT",
    "New Orleans Arena": "New Orleans, LA",
    "Rose Garden Arena": "Portland, OR",
    "Amway Arena": "Orlando, FL",
    "Oklahoma City Arena": "Oklahoma City, OK",
    "Wachovia Center": "Philadelphia 76ers",
    "Izod Center": "Brooklyn Nets",
    "Conseco Fieldhouse": "Indianapolis, IN",
    "Bradley Center": "Milwaukee, WI",
    "Power Balance Pavilion": "Sacramento, CA",
    "AccorHotels Arena": "Paris, FR",
    "Alamodome": "San Antonio, TX",
    "Prudential Center": "Newark, NJ",
    "Moody Center": "Austin, TX",
    "T-Mobile Arena": "Las Vegas, NV",
    "AccorHotels Arena": "Paris, FR",
    "Alamodome": "San Antonio, TX",
    "Alexander Memorial Coliseum at McDonald's Center": "Atlanta, GA",
    "America West Arena": "Phoenix, AZ",
    "ARCO Arena (II)": "Sacramento, CA",
    "Arrowhead Pond": "Anaheim, CA",
    "Charlotte Bobcats Arena": "Charlotte, NC",
    "Charlotte Coliseum": "Charlotte, NC",
    "Compaq Center": "Houston, TX",
    "Continental Airlines Arena": "East Rutherford, NJ",
    "First Union Center": "Philadelphia, PA",
    "FleetCenter": "Boston, MA",
    "Ford Center": "Oklahoma City, OK",
    "General Motors Place": "Vancouver, BC",
    "Georgia Dome": "Atlanta, GA",
    "Great Western Forum": "Inglewood, CA",
    "Gund Arena": "Cleveland, OH",
    "KeyArena at Seattle Center": "Seattle, WA",
    "LLoyd Noble Center": "Oklahoma City, OK",
    "Los Angeles Memorial Sports Arena": "Los Angeles, CA",
    "Maple Leaf Gardens": "Toronto, ON",
    "Market Square Arena": "Indianapolis, IN",
    "MCI Center": "Washington, DC",
    "McNichols Sports Arena": "Denver, CO",
    "Memorial Coliseum": "Portland, OR",
    "Miami Arena": "Miami, FL",
    "Moody Center": "Austin, TX",
    "Oakland Arena": "Oakland, CA",
    "Orlando Arena": "Orlando, FL",
    "Pete Maravich Assembly Center": "Baton Rouge, LA",
    "Pyramid Arena": "Memphis, TN",
    "Reunion Arena": "Dallas, TX",
    "Saitama Super Arena": "Saitama, JP",
    "SBC Center": "San Antonio, TX",
    "SkyDome": "Toronto, ON",
    "T-Mobile Arena": "Las Vegas, NV",
    "TD Banknorth Garden": "Boston, MA",
    "TD Waterhouse Centre": "Orlando, FL",
    "The Arena in Oakland": "Oakland, CA",
    "Tokyo Dome": "Tokyo, JP",
    "Anaheim Convention Center": "Anaheim, CA",
    "ARCO Arena (I)": "Sacramento, CA",
    "Arizona Veterans Memorial Coliseum": "Phoenix, AZ",
    "Baltimore Arena": "Baltimore, MD",
    "Boston Garden": "Boston, MA",
    "Brendan Byrne Arena": "East Rutherford, NJ",
    "Buffalo Memorial Auditorium": "Buffalo, NY",
    "Capital Centre": "Washington, DC",
    "Checkerdome": "St. Louis, MO",
    "Chicago Stadium": "Chicago, IL",
    "Cobo Arena": "Detroit, MI",
    "Coliseum at Richfield": "Cleveland, OH",
    "Copps Coliseum": "Hamilton, ON",
    "CoreStates Center": "Philadelphia, PA",
    "CoreStates Spectrum": "Philadelphia, PA",
    "Cow Palace": "San Francisco, CA",
    "Dane County Veteran Memorial Coliseum": "Madison, WI",
    "Freedom Hall": "Louisville, KY",
    "Hartford Civic Center": "Hartford, CT",
    "Hec Edmunson Pavilion": "Seattle, WA",
    "HemisFair Arena": "San Antonio, TX",
    "Hofheinz Pavilion": "Houston, TX",
    "Hubert H. Humphrey Metrodome": "Minneapolis, MN",
    "Jadwin Gymnasium": "Princeton, NJ",
    "Joe Louis Arena": "Detroit, MI",
    "Kemper Arena": "Kansas City, MO",
    "Lakefront Arena": "New Orleans, LA",
    "Louisiana Superdome": "New Orleans, LA",
    "Loyola Field House": "New Orleans, LA",
    "LSU Assembly Center": "Baton Rouge, LA",
    "MECCA Arena": "Milwaukee, WI",
    "Mississippi Coast Coliseum": "Biloxi, MS",
    "Moody Coliseum": "Dallas, TX",
    "Municipal Auditorium": "New Orleans, LA",
    "Nassau Veterans Memorial Coliseum": "New York City, NY",
    "Oakland-Alameda County Coliseum Arena": "Oakland, CA",
    "Omaha Civic Auditorium": "Omaha, NE",
    "Omni Coliseum": "Atlanta, GA",
    "Palacio de los Deportes": "Mexico City, MX",
    "Pontiac Silverdome": "Detroit, MI",
    "Providence Civic Center": "Providence, RI",
    "Riverside Centroplex": "Baton Rouge, LA",
    "Rutgers Athletic Center": "Piscataway, NJ",
    "Salt Palace": "Salt Lake City, UT",
    "San Diego Sports Arena": "San Diego, CA",
    "San Jose Arena": "San Jose, CA",
    "Seattle Center Coliseum": "Seattle, WA",
    "Seattle Kingdome": "Seattle, WA",
    "Springfield Civic Center": "Springfield, MA",
    "Tacoma Dome": "Tacoma, WA",
    "The Forum": "Los Angeles, CA",
    "The Spectrum": "Philadelphia, PA",
    "The Summit": "Houston, TX",
    "Thomas & Mack Center": "Las Vegas, NV",
    "Tokyo Metropolitan Gymnasium": "Tokyo, JP",
    "University of Utah": "Salt Lake City, UT",
    "US Airways Arena": "Washington, DC",
    "USAir Arena": "Washington, DC",
    "Yokohama Arena": "Yokohama, JP",
    "Intuit Dome": "Los Angeles, CA",
    "Rocket Arena": "Cleveland, OH"
}

PLAYOFF_START_DATES = {
    2024: "2024-04-20",
    2023: "2023-04-15",
    2022: "2022-04-16",
    2021: "2021-05-22",
    2020: "2020-08-17",
    2019: "2019-04-13",
    2018: "2018-04-14",
    2017: "2017-04-15",
    2016: "2016-04-16",
    2015: "2015-04-18",
    2014: "2014-04-19",
    2013: "2013-04-20",
    2012: "2012-04-28",
    2011: "2011-04-16",
    2010: "2010-04-17",
    2009: "2009-04-18",
    2008: "2008-04-19",
    2007: "2007-04-21",
    2006: "2006-04-22",
    2005: "2005-04-23",
    2004: "2004-04-17",
    2003: "2003-04-19",
    2002: "2002-04-20",
    2001: "2001-04-21",
    2000: "2000-04-22",
    1999: "1999-05-08",
    1998: "1998-04-23",
    1997: "1997-04-24",
    1996: "1996-04-25",
    1995: "1995-04-27",
    1994: "1994-04-28",
    1993: "1993-04-29",
    1992: "1992-04-23",
    1991: "1991-04-25",
    1990: "1990-04-26",
    1989: "1989-04-27",
    1988: "1988-04-28",
    1987: "1987-04-23",
    1986: "1986-04-17",
    1985: "1985-04-17",
    1984: "1984-04-17",
    1983: "1983-04-19",
    1982: "1982-04-20",
    1981: "1981-03-31",
    1980: "1980-04-02",
    1979: "1979-04-10",
    1978: "1978-04-11",
    1977: "1977-04-12",
    1976: "1976-04-13",
    1975: "1975-04-08"
}

# RETRIEVE DATA FROM DATABASE #
def get_existing_player_ids():
    connection = get_connection()  # Use shared connection
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT player_id FROM players;")
            rows = cursor.fetchall()
            return {row['player_id'] for row in rows}
    except Exception as e:
        print(f"Error fetching player IDs: {e}")
        return set()

def get_most_recent_season():
    connection = get_connection()  # Use shared connection
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT MAX(season) FROM games;")
        max_season = cursor.fetchone()[0]

        # Determine the current year and month
        current_year = datetime.now().year
        current_month = datetime.now().month

        # Decide which season to use
        if current_month >= 7 and current_year == max_season:
            return max_season + 1
        else:
            return max_season
    except Exception as e:
        print(f"Error fetching most recent season: {e}")
        return None

def get_existing_game_ids():
    connection = get_connection()
    try:
        cursor = connection.cursor()
        query = """
        SELECT DISTINCT game_id FROM player_boxscores
        UNION
        SELECT DISTINCT game_id FROM team_boxscores
        UNION
        SELECT DISTINCT game_id FROM staging_player_boxscores
        UNION
        SELECT DISTINCT game_id FROM staging_team_boxscores
        UNION
        SELECT DISTINCT game_id FROM games
        UNION
        SELECT DISTINCT game_id FROM staging_games
        """
        cursor.execute(query)
        result = cursor.fetchall()
        return {row[0] for row in result}  # Return a set of game_ids
    except Exception as e:
        print(f"Error fetching game_ids from all tables: {e}")
        raise
    finally:
        cursor.close()

def get_existing_nonstaging_game_ids():
    connection = get_connection()
    try:
        cursor = connection.cursor()
        query = """
        SELECT DISTINCT game_id FROM player_boxscores
        UNION
        SELECT DISTINCT game_id FROM team_boxscores
        UNION
        SELECT DISTINCT game_id FROM games
        UNION
        SELECT DISTINCT game_id FROM staging_player_boxscores
        UNION
        SELECT DISTINCT game_id FROM staging_team_boxscores
        """
        cursor.execute(query)
        result = cursor.fetchall()
        return {row[0] for row in result}  # Return a set of game_ids
    except Exception as e:
        print(f"Error fetching game_ids from all tables: {e}")
        raise
    finally:
        cursor.close()

def get_most_recent_date():
    # Use your `get_connection` function to fetch the max season
    connection = get_connection()
    try:
        cursor = connection.cursor()
        query = """ SELECT MAX(max_date) AS overall_max_date
                        FROM (
                        SELECT MAX(date) AS max_date FROM games
                        UNION ALL
                        SELECT MAX(date) AS max_date FROM staging_games
                    ) AS all_dates; """
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Exception as e:
        print(f"Error fetching game_ids from all tables: {e}")
        raise
    finally:
        cursor.close()

def get_most_recent_date_nonstaging():
    # Use your `get_connection` function to fetch the max season
    connection = get_connection()
    try:
        cursor = connection.cursor()
        query = """ SELECT MAX(max_date) AS overall_max_date
                        FROM (
                        SELECT MAX(date) AS max_date FROM games
                    ) AS all_dates; """
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Exception as e:
        print(f"Error fetching game_ids from all tables: {e}")
        raise
    finally:
        cursor.close()

# EDIT DATA FOR UPLOAD #
def parse_name(name):
    # Define common name suffixes and compound last name starters
    suffixes = {"Jr.", "Sr.", "II", "III", "IV"}
    compound_last_name_starters = {"Del", "Van", "Da", }

    # Split the name by spaces
    name_parts = name.strip().split()

    # Initialize the variables
    first_name = ""
    other_name = ""
    last_name = ""
    name_suffix = ""

    # Handle suffixes
    if name_parts[-1] in suffixes:
        name_suffix = name_parts.pop()  # Remove the suffix from the parts

    # Handle compound last names like "Del Negro", "Van Horn", "Da Silva"
    # Check if the last part contains compound name parts like "Del", "Van", or "Da"
    if len(name_parts) > 1 and name_parts[-2] in compound_last_name_starters:
        last_name = " ".join(name_parts[-2:])  # Join the last two parts as the last name
        name_parts = name_parts[:-2]  # Remove the last two parts from the name_parts list
    elif len(name_parts) > 1:
        last_name = name_parts.pop()  # Last name is the last part

    # The first name is the first part
    if len(name_parts) > 0:
        first_name = name_parts[0]

    # The rest is considered the middle name or initial(s)
    if len(name_parts) > 1:
        other_name = " ".join(name_parts[1:])  # Middle name or initial(s)

    return first_name, other_name, last_name, name_suffix

def get_country_name(country_code):
    country = pycountry.countries.get(alpha_2=country_code.upper())  # alpha_2 for 2-letter code
    return country.name if country else 'Unknown'

def extract_draft_info(data):
    # Define a regex pattern to match the round, pick, and overall numbers
    pattern = r"(\d+)[a-z]{2} round \((\d+)[a-z]{2} pick, (\d+)[a-z]{2} overall\)"
    match = re.search(pattern, data)
    if match:
        draft_round = int(match.group(1))
        draft_pick = int(match.group(2))
        draft_overall = int(match.group(3))
        return draft_round, draft_pick, draft_overall
    else:
        return None, None, None

def filtered_games(games, most_recent_game_date, database_games):

    today = datetime.now().date()
    most_recent_game_date = most_recent_game_date[0][0]
    # Filter games
    filtered = [
        game for game in games
        if (
            most_recent_game_date < datetime.strptime(game["date"], "%Y-%m-%d").date() < today
            and game["game_id"] not in database_games
        )
    ]

    return filtered

# DATA SCRAPING #

def get_all_player_ids():
# Function to get player IDs
    player_ids = []
    base_url = "https://www.basketball-reference.com/players/"
    for letter in 'abcdefghijklmnopqrstuvwxyz':
        time.sleep(2)
        # Fetch the page for the current letter
        url = f"{base_url}{letter}/"
        response = requests.get(url, timeout=(10, 120))
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the players table
        table = soup.find('table', id='players')
        if not table:
            continue  # Skip if table not found
        
        rows = table.find('tbody').find_all('tr')
        
        # Loop through the rows of the table
        for row in rows:
            # Skip header rows
            if 'thead' in row.get('class', []):
                continue
            
            # Extract player ID
            player_id = row.find('th')['data-append-csv']
            player_ids.append(player_id)
    print('Finished scraping player tables.')
    return player_ids

def scrape_new_player_details(new_player_ids):
# Function to scrape details of new players
    players_data = []
    base_url = "https://www.basketball-reference.com/players/"
    
    for player_id in new_player_ids:
        # Get the first letter of the player ID
        first_letter = player_id[0]
        player_url = f"{base_url}{first_letter}/{player_id}.html"
        response = requests.get(player_url, timeout=(10, 120))
        soup = BeautifulSoup(response.text, 'html.parser')
        # Add a delay to avoid overwhelming the server
        time.sleep(2)

        infodiv = soup.find('div', id='meta')
        if not infodiv:
            continue  # Skip if not found

        h1_tag = infodiv.find('h1')
        if h1_tag:
            name = h1_tag.text.strip()
            p_tags = h1_tag.find_all_next('p')
            if p_tags:
                # Check for pronunciation and skip if present
                first_p = p_tags[0]
                if first_p.find('strong') and 'pronunciation' in first_p.text.lower():
                    first_p = p_tags[1]
                strong_tag = first_p.find('strong')
                if strong_tag:
                    full_name = strong_tag.text.strip()
                # Extract dominant hand
        dominant_hand = None
        shoots_p = soup.find('strong', string=lambda x: x and "Shoots:" in x)

        if shoots_p:
            # Get the next sibling of the strong tag (which should be the text we want)
            next_sibling = shoots_p.find_next_sibling(string=True).strip()

            if next_sibling:
                dominant_hand = next_sibling

        colleges_strong = infodiv.find('strong', string=lambda x: x and "College" in x)
        colleges = []
        if colleges_strong:
            colleges_p = colleges_strong.find_parent('p')
            a_tags = colleges_p.find_all('a')
            colleges = [a.text for a in a_tags]


        # Extract birth location
        birth_location = None
        birth_city = 'Unknown'
        birth_state = 'Unknown'
        birth_country = 'Unknown'
        born_strong = soup.find('strong', string=lambda x: x and "Born:" in x)
        if born_strong:
            # Find the specific span containing the location
            born_p = born_strong.find_parent('p')
            if born_p:
                location_span = born_p.find_all('span')
                for span in location_span:
                    if "in" in span.text:
                        location_text = span.text.strip()
                        # Extract the location part after "in"
                        birth_location = location_text.split("in ")[-1].strip()
                        if birth_location:
                            parts = birth_location.split(",")
                            if len(parts) == 2:  # Ensure the location has both city and state
                                birth_city = parts[0].strip()[3:]
                                birth_state = parts[1].strip()
                        break
                birth_date_span = born_p.find('span', {'id': 'necro-birth'})
                if birth_date_span:
                    birth_date = birth_date_span.get('data-birth')

                # Extract the birth country from the <span> with class "f-i f-{country_code}"
                country_span = born_p.find('span', class_=lambda x: x and x.startswith('f-i f-'))
                if country_span and country_span.text.strip():
                    birth_country = country_span.text.strip()
                    birth_country = get_country_name(birth_country)

        # Extract draft details
        draft_round = draft_pick = draft_pick_overall = draft_year = 0
        draft_team = 'Undrafted'
        draft_strong = soup.find('strong', string=lambda x: x and "Draft:" in x)
        if draft_strong:
            draft_p = draft_strong.find_parent('p')
            if draft_p:
                draft_a_tag = draft_p.find('a')
                if draft_a_tag and draft_a_tag.next_sibling.strip():
                    draft_info = draft_a_tag.next_sibling.strip()
                    draft_year = draft_a_tag.next_sibling.text.strip()
                    draft_round, draft_pick, draft_pick_overall = extract_draft_info(draft_info)
                    draft_team = draft_a_tag.text.strip()
                    # Extract draft year from the second <a> tag containing "NBA Draft"
                    draft_year_tag = draft_p.find('a', href=lambda x: x and "NBA" in x)
                    if draft_year_tag:
                        # Ensure draft_year is set to a numeric value or None
                        draft_year_text = draft_year_tag.text.strip()
                        if draft_year_text[:1].isdigit():
                            draft_year = int(draft_year_text[:4])
                        else:
                            draft_year = None  # Invalid year, set to None
        
        # Extract height and weight
        measurement_p = None
        for p in infodiv.find_all('p'):
            spans = p.find_all('span')
            if len(spans) == 2:  # Check for at least two <span> elements
                measurement_p = p
                break
        height = 0 
        weight = 0
        if measurement_p:
            height_text = measurement_p.find_all('span')[0].text
            weight_text = measurement_p.find_all('span')[1].text
            feet, inches = map(int, height_text.split('-'))
            height = feet * 12 + inches  # Convert to total inches
            weight = int(weight_text.replace('lb', ''))

        first_name, other_name, last_name, name_suffix = parse_name(name)
        full_first_name, full_other_name, full_last_name, full_name_suffix = parse_name(full_name)

        players_data.append({
            "player_id": player_id,
            "name": name,
            "first_name": first_name,
            "other_name": other_name,
            "last_name": last_name,
            "name_suffix": name_suffix,
            "full_name": full_name,
            "full_first_name": full_first_name,
            "full_other_name": full_other_name,
            "full_last_name": full_last_name,
            "full_name_suffix": full_name_suffix,
            "height": height,
            "weight": weight,
            "birth_date": birth_date,
            "birth_city": birth_city,
            "birth_state": birth_state,
            "birth_country": birth_country,
            "colleges": colleges,
            "draft_team": draft_team,
            "draft_round": draft_round,
            "draft_pick": draft_pick,
            "draft_pick_overall": draft_pick_overall,
            "draft_year": draft_year,
            "dominant_hand": dominant_hand
        })

    return players_data

def fetch_months(year):
    url = f"https://www.basketball-reference.com/leagues/NBA_{year}_games.html"
    time.sleep(2)  # Add delay before making a request
    response = requests.get(url)
    response.raise_for_status()  # Ensure request was successful
    soup = BeautifulSoup(response.content, "html.parser")
    months = []

    filter_div = soup.find("div", class_="filter")
    if filter_div:
        for link in filter_div.find_all("a"):
            month = link.text.strip().lower()
            months.append(month)

    return months

def scrape_month(year, month):
    url = f"https://www.basketball-reference.com/leagues/NBA_{year}_games-{month}.html"
    time.sleep(2)  # Add delay before making a request
    response = requests.get(url)
    response.raise_for_status()  # Ensure request was successful
    soup = BeautifulSoup(response.content, "html.parser")
    games = []
    team_box_scores = []

    tbody = soup.find("tbody")
    if not tbody:
        return games

    for row in tbody.find_all("tr"):
        if "thead" in row.get("class", []):
            continue

        game = {}
        date_cell = row.find("th", {"data-stat": "date_game"})
        if date_cell:
            date_str = date_cell.text.strip()
            game["date"] = datetime.strptime(date_str, "%a, %b %d, %Y").strftime("%Y-%m-%d")

        visitor_team_cell = row.find("td", {"data-stat": "visitor_team_name"})
        if visitor_team_cell:
            game["away_team_id"] = TEAM_NAME_TO_ID.get(visitor_team_cell.text.strip(), "Unknown")
            game["away_team_name"] = visitor_team_cell.text.strip()

        visitor_pts_cell = row.find("td", {"data-stat": "visitor_pts"})
        if visitor_pts_cell:
            game["away_team_pts"] = visitor_pts_cell.text.strip()

        home_team_cell = row.find("td", {"data-stat": "home_team_name"})
        if home_team_cell:
            game["home_team_id"] = TEAM_NAME_TO_ID.get(home_team_cell.text.strip(), "Unknown")
            game["home_team_name"] = home_team_cell.text.strip()

        home_pts_cell = row.find("td", {"data-stat": "home_pts"})
        if home_pts_cell:
            game["home_team_pts"] = home_pts_cell.text.strip()

        ot_cell = row.find("td", {"data-stat": "overtimes"})
        game["ot"] = ot_cell.text.strip() if ot_cell else "REG"
        if game["ot"] == "": game["ot"] = "REG"

        venue_cell = row.find("td", {"data-stat": "arena_name"})
        game["venue"] = venue_cell.text.strip() if venue_cell else ""
        game["location"] = VENUE_TO_LOCATION.get(game["venue"], "Unknown")

        remarks_cell = row.find("td", {"data-stat": "game_remarks"})
        if remarks_cell:
            remarks_text = remarks_cell.text.strip()
            if remarks_text == "Play-In Game":
                game["game_type"] = "Play-In"
            elif game["date"] >= PLAYOFF_START_DATES.get(year, "9999-12-31"):
                game["game_type"] = "Playoffs"
            else:
                game["game_type"] = "Regular Season"
        else:
            game["game_type"] = "Regular Season"

        game["season"] = year
        game["game_id"] = f"{game['home_team_id']}{game['away_team_id']}{game['date'].replace('-', '')}"

        url_cell = row.find("td", {"data-stat": "box_score_text"})
        if url_cell:
            href_tag = url_cell.find('a')
            if href_tag:
                game['url_link'] = "https://www.basketball-reference.com/" + href_tag.get('href')

        games.append(game)

    return games

def scrape_box_score(game_id, url, retries=5):
    response = None

    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=(10, 120))
            response.raise_for_status()  # Raise an exception for HTTP errors
            break  # Exit the retry loop if the request is successful
        except requests.exceptions.Timeout as e:
            print(f"Timeout on attempt {attempt + 1}: {e}")
            sleep(2 ** attempt)  # Exponential backoff
        except requests.exceptions.RequestException as e:
            print(f"Request error on attempt {attempt + 1}: {e}")
            sleep(2 ** attempt)  # Exponential backoff

    if response is None:
        raise Exception("Max retries reached. Unable to fetch the box score.")

    soup = BeautifulSoup(response.content, "html.parser")

    # Parse the team IDs from the game_id (e.g., "202204120ATL-CLE")
    home_team_id = game_id[:3]
    away_team_id = game_id[3:6]

    # Find box score tables for both teams
    box_score_tables = soup.find_all('table', id=re.compile(r'box-[A-Z]{3}-game-basic'))
    if len(box_score_tables) < 2:
        return [], []  # Return empty lists if box scores are incomplete

    team_box_scores = []
    player_box_scores = []

    for i, box_score_table in enumerate(box_score_tables):
        # TEAM BOX SCORE
        team_box_score = {}
        team_id = away_team_id if i == 0 else home_team_id
        unique_id = game_id + "-" + team_id
        team_box_score["_id"] = unique_id
        team_box_score["game_id"] = game_id
        team_box_score["team_id"] = away_team_id if i == 0 else home_team_id  # Assign team_id

        if team_box_score["team_id"] == away_team_id:
            team_box_score["home_away"] = "Away"
        elif team_box_score["team_id"] == home_team_id:
            team_box_score["home_away"] = "Home"

        # Locate the footer row with team stats
        footer_row = box_score_table.find("tfoot").find("tr")
        if footer_row:
            for stat in ["mp", "fg", "fga", "fg3", "fg3a", "ft", "fta", 
                         "orb", "drb", "trb", "ast", "stl", "blk", "tov", "pf", "pts"]:
                stat_cell = footer_row.find("td", {"data-stat": stat})
                if stat == "mp" and stat_cell:
                    team_box_score[stat] = int(stat_cell.text.strip()) * 60  # Convert minutes to seconds
                else:
                    team_box_score[stat] = stat_cell.text.strip() if stat_cell else None

        team_box_scores.append(team_box_score)

        # PLAYER BOX SCORES
        body_rows = box_score_table.find("tbody").find_all("tr", class_=lambda c: c != "thead")
        for row in body_rows:
            

            # Extract player ID from <th>
            player_cell = row.find("th", {"data-stat": "player"})
            if player_cell:
                player_id = player_cell.get("data-append-csv")

            player_box_score = {"_id": game_id + "-" + team_box_score["team_id"] + "-" + player_id, "game_id": game_id, "team_id": team_box_score["team_id"], "player_id": player_id}

            # Extract `mp` in seconds from the <td> element
            mp_cell = row.find("td", {"data-stat": "mp"})
            mp_csk = mp_cell.get("csk") if mp_cell else None
            if mp_csk and mp_csk.isdigit():
                player_box_score["mp"] = int(mp_csk)
            else:
                player_box_score["mp"] = 0  # Default to 0 if `csk` is missing or invalid

            # Check for "Did not play"
            reason_cell = row.find("td", {"data-stat": "reason"})
            if reason_cell and "Did not play" in reason_cell.text:
                for stat in ["fg", "fga", "fg3", "fg3a", "ft", "fta", 
                             "orb", "drb", "trb", "ast", "stl", "blk", "tov", "pf", "pts"]:
                    player_box_score[stat] = 0
            else:
                for stat in ["fg", "fga", "fg3", "fg3a", "ft", "fta", 
                             "orb", "drb", "trb", "ast", "stl", "blk", "tov", "pf", "pts"]:
                    stat_cell = row.find("td", {"data-stat": stat})
                    player_box_score[stat] = stat_cell.text.strip() if stat_cell else None

            player_box_scores.append(player_box_score)

    return team_box_scores, player_box_scores

# UPDATE DATABASE #

def insert_into_players_table(players_data):
    connection = get_connection()  # Use shared connection
    try:
        # Start a cursor
        cursor = connection.cursor()
        
        # Define the INSERT query
        insert_query = """
        INSERT INTO players (
            player_id, name, first_name, other_name, last_name, name_suffix,
            full_name, full_first_name, full_other_name, full_last_name, full_name_suffix,
            height, weight, birth_date, birth_city, birth_state, birth_country,
            colleges, draft_team, draft_round, draft_pick, draft_pick_overall,
            draft_year, dominant_hand
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Prepare the data as a list of tuples
        insert_data = [
            (
                player['player_id'],
                player['name'],
                player['first_name'],
                player['other_name'],
                player['last_name'],
                player['name_suffix'],
                player['full_name'],
                player['full_first_name'],
                player['full_other_name'],
                player['full_last_name'],
                player['full_name_suffix'],
                player.get('height'),
                player.get('weight'),
                player['birth_date'],
                player['birth_city'],
                player['birth_state'],
                player['birth_country'],
                player['colleges'],
                player['draft_team'],
                player.get('draft_round'),
                player.get('draft_pick'),
                player.get('draft_pick_overall'),
                player.get('draft_year'),
                player['dominant_hand']
            )
            for player in players_data
        ]
        
        # Execute the query for all rows
        cursor.executemany(insert_query, insert_data)

        # Commit the transaction
        connection.commit()
        print(f"Inserted {cursor.rowcount} rows successfully.")

    except Exception as e:
        print(f"Error: {e}")
        connection.rollback()
        raise
    finally:
        # Close the cursor
        cursor.close()

def insert_into_games_table(games_data):
    connection = get_connection()
    try:
        # Start a cursor
        cursor = connection.cursor()
        
        # Define the INSERT query
        insert_query = """
        INSERT INTO staging_games (
            game_id, date, away_team_id, away_team_name, away_team_pts, home_team_id, home_team_name, home_team_pts, ot, venue, location, game_type, season
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Prepare the data as a list of tuples
        insert_data = [
            (
                game['game_id'],
                game['date'],
                game['away_team_id'],
                game['away_team_name'],
                game['away_team_pts'],
                game['home_team_id'],
                game['home_team_name'],
                game['home_team_pts'],
                game['ot'],
                game['venue'],
                game['location'],
                game['game_type'],
                game['season'],

            )
            for game in games_data
        ]
        
        # Execute the query for all rows
        cursor.executemany(insert_query, insert_data)

        # Commit the transaction
        connection.commit()
        print(f"Inserted {cursor.rowcount} rows successfully.")

    except Exception as e:
        print(f"Error: {e}")
        connection.rollback()
        raise
    finally:
        # Close the cursor
        cursor.close()

def insert_into_team_boxscores_table(data):
    connection = get_connection()
    try:
        # Start a cursor
        cursor = connection.cursor()
        
        # Define the INSERT query
        insert_query = """
        INSERT INTO staging_team_boxscores (
            _id, game_id, team_id, home_away, mp, fg, fga, fg3, fg3a, ft, fta, orb, drb, trb, ast, stl, blk, tov, pf, pts
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Prepare the data as a list of tuples
        insert_data = [
            (
                game['_id'],
                game['game_id'],
                game['team_id'],
                game['home_away'],
                game['mp'],
                game['fg'],
                game['fga'],
                game['fg3'],
                game['fg3a'],
                game['ft'],
                game['fta'],
                game['orb'],
                game['drb'],
                game['trb'],
                game['ast'],
                game['stl'],
                game['blk'],
                game['tov'],
                game['pf'],
                game['pts'],
            )
            for game in data
        ]
        
        # Execute the query for all rows
        cursor.executemany(insert_query, insert_data)

        # Commit the transaction
        connection.commit()
        print(f"Inserted {cursor.rowcount} rows of team boxscores successfully.")

    except Exception as e:
        print(f"Error: {e}")
        connection.rollback()
        raise
    finally:
        # Close the cursor
        cursor.close()

def insert_into_player_boxscores_table(data):
    connection = get_connection()
    try:
        # Start a cursor
        cursor = connection.cursor()
        
        # Define the INSERT query
        insert_query = """
        INSERT INTO staging_player_boxscores (
            _id, game_id, team_id, player_id, mp, fg, fga, fg3, fg3a, ft, fta, orb, drb, trb, ast, stl, blk, tov, pf, pts
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Prepare the data as a list of tuples
        insert_data = [
            (
                game['_id'],
                game['game_id'],
                game['team_id'],
                game['player_id'],
                game['mp'],
                game['fg'],
                game['fga'],
                game['fg3'],
                game['fg3a'],
                game['ft'],
                game['fta'],
                game['orb'],
                game['drb'],
                game['trb'],
                game['ast'],
                game['stl'],
                game['blk'],
                game['tov'],
                game['pf'],
                game['pts'],
            )
            for game in data
        ]
        
        # Execute the query for all rows
        cursor.executemany(insert_query, insert_data)

        # Commit the transaction
        connection.commit()
        print(f"Inserted {cursor.rowcount} rows of player boxscores successfully.")

    except Exception as e:
        print(f"Error: {e}")
        connection.rollback()
        raise
    finally:
        # Close the cursor
        cursor.close()

# FUNCTIONS FOR EACH UPDATE #

def update_players():
    print("Updating players...")
    # Step 1: Get the list of all player IDs
    all_player_ids = get_all_player_ids()
    print(f"Total player IDs found: {len(all_player_ids)}")

    # Step 2: Load existing player IDs from your database
    existing_player_ids = get_existing_player_ids()
    
    # Step 3: Find new players
    new_player_ids = list(set(all_player_ids) - set(existing_player_ids))
    print(f"New player IDs found: {len(new_player_ids)}")

    # Step 4: Scrape details for new players
    new_players_data = scrape_new_player_details(new_player_ids)
    if len(new_player_ids)<1:
        print("No new players to add")
        return

    print(new_player_ids)

    db_update_response = input("Update database with new player values? (y/n): ").strip().lower()

    if db_update_response in ["y", "yes"]:
        try:
        # Insert the data into the table
            insert_into_players_table(new_players_data)
        except Exception as e:
            print(f"Error Inserting Data Into Players Table: {e}")
    else:
        print("Update of players cancelled")
        exit()

def update_games():
    print("Updating games...")
    year = get_most_recent_season()
    all_games = []
    # Get today's date
    today = datetime.now().date()
    months = fetch_months(year)

    for month in months:
        games = scrape_month(year, month)
        all_games.extend(games)

    existing_game_ids = get_existing_game_ids()
    filtered_games = [
    game for game in all_games
        if datetime.strptime(game["date"], "%Y-%m-%d").date() < today
        and game["game_id"] not in existing_game_ids
    ]  
    filtered_games = [
    {"game_id": game["game_id"], **{k: v for k, v in game.items() if k != "game_id"}}
    for game in filtered_games
    ]
    
    print(f"Number of new games: {len(filtered_games)}")
    
    if len(filtered_games) > 0:
        unknown_venues = [row['venue'] for row in filtered_games if row['location'] == 'Unknown']
        if any(row['location'] == 'Unknown' for row in filtered_games):
            print("Update of games cancelled, following venue locations unknown: ", unknown_venues)
            exit()
        
        pprint.pprint(filtered_games)
        
        proceed_response = input("Update database staging table with new game values? (y/n): ").strip().lower()
        if proceed_response in ["y", "yes"]:
            try:
        # Insert the data into the table
                insert_into_games_table(filtered_games)
            except Exception as e:
                print(f"Error Inserting Data Into Games Table: {e}")
        else:
            print("Update of games cancelled")
            exit()

def update_boxscores():
    print("Updating boxscores...")
    all_games = []
    all_team_box_scores = []
    all_player_box_scores = []
    start_year=datetime.now().year
    end_year=datetime.now().year

    for year in range(start_year, end_year + 1):
        months = fetch_months(year)

        for month in months:
            games = scrape_month(year, month)
            all_games.extend(games)

            all_games = filtered_games(games, get_most_recent_date_nonstaging(), get_existing_nonstaging_game_ids())
            for game in all_games:
                if "url_link" in game:
                    team_box_scores, player_box_scores = scrape_box_score(game['game_id'], game['url_link'])
                    
                    # Append results to respective lists
                    all_team_box_scores.extend(team_box_scores)
                    all_player_box_scores.extend(player_box_scores)
                    
                    time.sleep(2)  # Avoid hitting the site too quickly

    if len(all_team_box_scores)==0 or len(all_player_box_scores)==0:
        print('No new boxscores to add')
        return
    print("Team Box Scores:")
    for record in all_team_box_scores[:5]:  # Show a sample of the first 5
        pprint.pprint(record)  # You can adjust with PrettyPrinter settings if needed
    if len(all_team_box_scores) > 5:
        print(f"... and {len(all_team_box_scores) - 5} more records.\n")

    # Show a sample of the first 5 player box scores
    print("Player Box Scores:")
    for record in all_player_box_scores[:5]:  # Show a sample of the first 5
        pprint.pprint(record)  # You can adjust with PrettyPrinter settings if needed
    if len(all_player_box_scores) > 5:
        print(f"... and {len(all_player_box_scores) - 5} more records.")
    proceed_response = input("Update database staging tables with new boxscore values? (y/n): ").strip().lower()
    if proceed_response in ["y", "yes"]:
        try:
        # Insert the data into the table
            insert_into_team_boxscores_table(all_team_box_scores)
            insert_into_player_boxscores_table(all_player_box_scores)
        except Exception as e:
            print(f"Error Inserting Data Into Boxscore Tables: {e}")
    else:
        print("Update of boxscores cancelled")

def remove_duplicates_from_staging_tables():
    """
    Check for and optionally remove duplicates from specified staging tables in the database.
    """
    tables = [
        "staging_games",
        "staging_player_boxscores",
        "staging_team_boxscores"
    ]

    try:
        connection = get_connection()
        with connection.cursor() as cursor:
            for table in tables:
                # Get all column names
                cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}';")
                columns = [row[0] for row in cursor.fetchall()]
                column_list = ", ".join(columns)

                # Count duplicates
                print(f"Checking for duplicates in {table}...")
                count_query = f"""
                    SELECT COUNT(*) - COUNT(DISTINCT ROW({column_list}))
                    FROM {table};
                """
                cursor.execute(count_query)
                duplicates_count = cursor.fetchone()[0]

                if duplicates_count > 0:
                    print(f"There are {duplicates_count} duplicate rows in {table}.")
                    user_input = input("Do you want to remove these duplicates? (y/n): ").strip().lower()
                    if user_input == 'y':
                        # Delete duplicates
                        delete_query = f"""
                            DELETE FROM {table}
                            WHERE ctid NOT IN (
                                SELECT MIN(ctid)
                                FROM {table}
                                GROUP BY {column_list}
                            );
                        """
                        cursor.execute(delete_query)
                        print(f"Duplicates removed from {table}.")
                    else:
                        print(f"Skipped removing duplicates from {table}.")
                else:
                    print(f"No duplicates found in {table}.")

            # Commit changes after processing all tables
            connection.commit()
            print("Duplicates removal process completed.")

    except Exception as e:
        print(f"Error while removing duplicates: {e}")
        if connection:
            connection.rollback()  # Roll back changes in case of an error

# FULL SCRIPT #        
if __name__ == "__main__":

    get_connection()

    update_players()
    
    update_games()

    update_boxscores()

    remove_duplicates_from_staging_tables()
    
    close_connection()
