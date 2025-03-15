import datetime

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

def get_stat_value(stats, target_stat_name):
    for stat in stats:
        if stat["stat_name"] == target_stat_name:
            return stat.get("value", None)
    return None

def get_operator_for_stat(stats, target_stat_name):
    # Iterate through the stats list
    for stat in stats:
        # Check if the stat_name matches the target
        if stat['stat_name'] == target_stat_name:
            # Return the operator
            return stat['operator']
    return None  # Return None if not found

def clean_combo(data):
    # Initialize the result list for the 14 possible lines
    lines = []

    # Line 1: {Player Name} is the first [{dominant_hand}-handed] [{team_id}] [undrafted] player
    player_name = data['player_name']
    player_id = next(iter(data['player_id']))
    stats = data['stats']
    dominant_hand = get_stat_value(stats, 'dominant_hand')
    team_id = get_stat_value(stats, 'team_id')
    draft_round = get_stat_value(stats, 'draft_round')
    if draft_round:
        draft_round = int(draft_round)
    draft_status = "undrafted" if draft_round == 0 else ""
    line_1 = f"{player_name} is the first"
    if dominant_hand:
        dominant_hand = dominant_hand.replace("'", "")
        line_1 += f" {dominant_hand}-handed"
    if draft_status:
        line_1 += f" {draft_status}"
    if team_id:
        team_id = team_id.replace("'", "")
        team_id = team_name_map.get(team_id, "Unknown Team")
        line_1 += f" {team_id}"
    
    if draft_round and draft_round != 0:
        if draft_round == 1:
            line_1 += " 1st round pick"
        if draft_round == 2:
            line_1 += " 2nd round pick"
        if draft_round == 3:
            line_1 += " 3rd round pick"
        if draft_round >= 4:
            line_1 += " {draft_round}th round pick"
    else:
        line_1 += " player"
    
    lines.append(line_1)

    # Line 2: since {season}
    season = get_stat_value(stats, 'season')
    if season:
        lines.append(f"since {season}")
    else:
        lines.append(f"since the NBA/ABA merger")
    

    # Line 3: [{age_at_time_of_game} or older/younger]
    age = get_stat_value(stats, 'age_at_time_of_game')
    age_operator = get_operator_for_stat(stats, 'age_at_time_of_game')
    if age:
        comparison = "older" if age_operator == ">=" else "younger"
        lines.append(f"{age} years old or {comparison}")



    # Line 4: [[measuring {height} or taller/shorter] [weighing {weight}lbs or more/less]]
    height = get_stat_value(stats, 'height')
    height_operator = get_operator_for_stat(stats, 'height')
    weight = get_stat_value(stats, 'weight')
    weight_operator = get_operator_for_stat(stats, 'weight')

    if height or weight:
        if height:
            height = int(height)
            feet = height // 12
            inches = height % 12
            height = f"{feet}'{inches}\""
            hw_text = f"measuring {height} or taller" if height and height_operator == ">=" else f"measuring {height} or shorter"
        if weight:
            hw_text = f"weighing {weight}lbs or more" if weight and weight_operator == ">=" else f"weighing {weight}lbs or less"
        lines.append(f"{hw_text}")

    # Line 5: named {first_name} {last_name}
    first_name = get_stat_value(stats, 'first_name')
    last_name = get_stat_value(stats, 'last_name')
    
    if first_name:
        first_name = first_name.replace("'", "")
        lines.append(f"named {first_name}")
    
    if last_name:
        last_name = last_name.replace("'", "")
        lines.append(f"with the last name of {last_name}")




    # Line 6: born [in {birth_city} {birth_state} {birth_country} {birth_month}] [on {birth_date}] [before/after {birth_year}]
    birth_sections = []
    birth_month = get_stat_value(stats, 'birth_month')
    birth_date = get_stat_value(stats, 'birth_date')
    birth_year = get_stat_value(stats, 'birth_year')
    birth_city = get_stat_value(stats, 'birth_city')
    birth_state = get_stat_value(stats, 'birth_state')
    birth_country = get_stat_value(stats, 'birth_country')

    if birth_city or birth_state or birth_country or birth_month:
        if birth_city:
            birth_city = birth_city.replace("'", "")
            birth_sections.append(f"in {birth_city}")
        if birth_state:
            birth_state = birth_state.replace("'", "")
            birth_sections.append(f"in {birth_state}")
        if birth_country:
            birth_country = birth_country.replace("'", "")
            birth_sections.append(f"in {birth_country}")
        if birth_month:
            birth_month = birth_month.replace("'", "")
            birth_sections.append(f"in {birth_month}")    
    if birth_date:
        birth_date = birth_date.replace("'", "")
        birth_sections.append(f"on {birth_date}")
    if birth_year:
        birth_year = int(birth_year)
        if get_operator_for_stat(stats,'birth_year') == "<=":
            birth_sections.append(f"before {birth_year}")
        else:
            birth_sections.append(f"after {birth_year}")
    if birth_sections:
        lines.append(f"that was born {' '.join(birth_sections)}")


    # Line 7: drafted [in the X {draft_round}] [in {draft_year} or later/earlier]
    
    draft_year = get_stat_value(stats, 'draft_year')
    
    if draft_year:
        draft_year = int(draft_year)
        draft_year_operator = get_operator_for_stat(stats, 'draft_year')
        
        if draft_year_operator == '>=':
            year_text = f"{draft_year} or later"
        else:
            year_text = f"{draft_year} or earlier"
        
        lines.append(f"drafted in {year_text}")
    
    # Line 8: [to record a double/triple double]
    double_double = get_stat_value(stats, 'double_double')
    triple_double = get_stat_value(stats, 'triple_double')
    if double_double:
        lines.append("to record a double double")
    if triple_double:
        lines.append("to record a triple double")




    # Line 9: [in a {home/road} {overtime} win/loss or game]
    player_home_away = get_stat_value(stats, 'player_home_away')
    player_win_lose = get_stat_value(stats, 'player_win_lose')
    overtime = get_stat_value(stats, 'overtime')
    game_type = data['game_type']
    game_type_lower = game_type.lower()
    if player_home_away or overtime or player_win_lose:
        
        if player_home_away: player_home_away = player_home_away.replace("'", "")
        if player_home_away and player_home_away == 'home':
            home_away = 'home'
        elif player_home_away and player_home_away == 'away':
            
            home_away = 'road'
        else: home_away = ''
        
        if overtime:
            if overtime == 'REG': 
                overtime = ''
            elif overtime != 'REG': 
                overtime = overtime.replace("'", "")
        
        if player_win_lose: # update for when it's a win/loss but no double/triple double
            win_lose = "win" if player_win_lose == "win" else "loss"
        else: win_lose = 'game'

        components = [home_away, overtime, win_lose]
        combined_text = " ".join(filter(None, components))

        if double_double or triple_double:
            lines.append(f"in a {game_type_lower} {combined_text}")
        else: lines.append(f"to play in a {game_type_lower} {combined_text}")
    else: lines.append(f"to play a {game_type_lower} game")

    # Line 9.5 [against the {opp_team_id}]

    opp_team_id = get_stat_value(stats, 'opp_team_id')

    if opp_team_id:      
        opp_team_id = opp_team_id.replace("'", "")
        opp_team_id = team_name_map.get(opp_team_id, "Unknown Team")
        lines.append(f"against the {opp_team_id}")
    
    # Line 10: [in {game_month} / on {game_date} / on a {game_day_of_week}]
    game_month = get_stat_value(stats, 'game_month')
    game_date = get_stat_value(stats, 'game_date')
    game_day_of_week = get_stat_value(stats, 'game_day_of_week')


    if game_month or game_date or game_day_of_week:
        if game_month:
            game_month = game_month.replace("'", "").strip()
            lines.append(f"in {game_month}")
        if game_date:
            game_date = game_date.replace("'", "").strip()
            lines.append(f"on {game_date}")
        if game_day_of_week:
            game_day_of_week = game_day_of_week.replace("'", "").strip()
            lines.append(f"on a {game_day_of_week}")

    # Line 11: [in {game_location} / at {venue}]
    game_location = get_stat_value(stats, 'game_location')
    venue = get_stat_value(stats, 'venue')

    if game_location or venue:
        if game_location:
            game_location = game_location.replace("'", "")
            lines.append(f"in {game_location}")
        if venue:
            venue = venue.replace("'", "")
            lines.append(f"at {venue}")



    # Line 12: [and record more than {stat}]
    stat_values = {
        'fg': get_stat_value(stats, 'fg'),
        'fg3': get_stat_value(stats, 'fg3'),
        'ft': get_stat_value(stats, 'ft'),
        'orb': get_stat_value(stats, 'orb'),
        'drb': get_stat_value(stats, 'drb'),
        'trb': get_stat_value(stats, 'trb'),
        'ast': get_stat_value(stats, 'ast'),
        'stl': get_stat_value(stats, 'stl'),
        'blk': get_stat_value(stats, 'blk'),
        'pts': get_stat_value(stats, 'pts')
    }

    stat_values_less_than = {
        'fga': get_stat_value(stats, 'fga'),
        'fg3a': get_stat_value(stats, 'fg3a'),
        'fta': get_stat_value(stats, 'fta'),
        'tov': get_stat_value(stats, 'tov'),
        'pf': get_stat_value(stats, 'pf')
    }

    stat_name_map = {
    'fg': 'field goal',
    'fg3': 'three-point field goal',
    'ft': 'free throw',
    'orb': 'offensive rebound',
    'drb': 'defensive rebound',
    'trb': 'total rebound',
    'ast': 'assist',
    'stl': 'steal',
    'blk': 'block',
    'pts': 'point',
    'fga': 'field goal attempts',
    'fg3a': 'three-point field goal attempts',
    'fta': 'free throw attempts',
    'tov': 'turnovers',
    'pf': 'personal fouls'
    }

    temp_lines = []
    # Loop through each stat and build the output
    for stat_name, value in stat_values.items():
        if value is not None:  # Ensure both value and operator exist
            descriptive_stat_name = stat_name_map.get(stat_name, stat_name)
            if value != '1':
                descriptive_stat_name += 's'
            if not temp_lines:
                temp_lines.append(f"at least {value} {descriptive_stat_name}")
            else:
                temp_lines.append(f"{value} {descriptive_stat_name}")
    
    if temp_lines:
        if len(temp_lines) == 1:
            stat_text = f"and record {temp_lines[0]}"
        elif len(temp_lines) == 2:
            stat_text = f"and record {temp_lines[0]} and {temp_lines[1]}"
        else:
            stat_text = (
                "and record " +
                ", ".join(temp_lines[:-1]) +  # Comma-separated list of all but the last stat
                f", and {temp_lines[-1]}"    # Add "and" before the last stat
            )
        lines.append(stat_text)

    # Print lines for debugging

    # Line 13: [with less than {stat}]
    temp_lines_less_than = []
    # Loop through each stat and build the output
    for stat_name, value in stat_values_less_than.items():
        if value is not None:  # Ensure both value and operator exist
            descriptive_stat_name = stat_name_map.get(stat_name, stat_name)
            if not temp_lines_less_than and value == '0':
                temp_lines_less_than.append(f"{value} {descriptive_stat_name}")
            elif temp_lines_less_than and value == '0':
                temp_lines_less_than.append(f"{value} {descriptive_stat_name}")
            else:
                temp_lines_less_than.append(f"{value} or fewer {descriptive_stat_name}")
    
    if temp_lines_less_than:
        if len(temp_lines_less_than) == 1:
            stat_text_less_than = f"while recording {temp_lines_less_than[0]}"
        if len(temp_lines_less_than) == 2:
            stat_text_less_than = f"while recording {temp_lines_less_than[0]} and {temp_lines_less_than[1]}"
        elif len(temp_lines_less_than) > 2:
            stat_text_less_than = (
                "while recording " +
                ", ".join(temp_lines_less_than[:-1]) +  # Comma-separated list of all but the last stat
                f", and {temp_lines_less_than[-1]}"    # Add "and" before the last stat
            )
        lines.append(stat_text_less_than)

    # Line 14: [and a {percentage stat name} greater than {percentage}]
    ts_pct = get_stat_value(stats, 'ts_pct')
    efg_pct = get_stat_value(stats, 'efg_pct')
    fg_pct = get_stat_value(stats, 'fg_pct')
    fg3_pct = get_stat_value(stats, 'fg3_pct')
    ft_pct = get_stat_value(stats, 'ft_pct')
    
    if ts_pct or efg_pct or fg_pct or fg3_pct or ft_pct:
        if ts_pct:
            ts_pct = float(ts_pct)
            ts_pct = ts_pct * 100
            ts_pct = int(ts_pct)
            if ts_pct == 150:
                lines.append(f"with a true shooting percentage of 150%")
            else: lines.append(f"with a true shooting percentage greater than {ts_pct}%")
        if efg_pct:
            efg_pct = float(efg_pct)
            efg_pct = efg_pct * 100
            efg_pct = int(efg_pct)
            if efg_pct == 150:
                lines.append(f"with an effective field goal percentage of 150%")
            else: lines.append(f"with an effective field goal percentage greater than {efg_pct}%")
        if fg_pct:
            fg_pct = float(fg_pct)
            fg_pct = fg_pct * 100
            fg_pct = int(fg_pct)
            if fg_pct == 100:
                lines.append(f"with a field goal percentage of 100%")
            else: lines.append(f"with a field goal percentage greater than {fg_pct}%")
        if fg3_pct:
            fg3_pct = float(fg3_pct)
            fg3_pct = fg3_pct * 100
            fg3_pct = int(fg3_pct)
            if fg_pct: pct_and = 'and'
            else: pct_and = 'with'
            if fg3_pct == 100:
                lines.append(f"{pct_and} a three point field goal percentage of 100%")
            else: lines.append(f"{pct_and} a three point field goal percentage greater than {fg3_pct}%")
        if ft_pct:
            ft_pct = float(ft_pct)
            ft_pct = ft_pct * 100
            ft_pct = int(ft_pct)
            if fg_pct or fg3_pct: ft_pct_and = 'and'
            else: ft_pct_and = 'with'
            if ft_pct == 100:
                lines.append(f"{ft_pct_and} a free throw percentage of 100%")
            else: lines.append(f"{ft_pct_and} a free throw percentage greater than {ft_pct}%")


    # Line 15: Minutes played
    mp = get_stat_value(stats, 'mp')
    if mp:
        lines.append(f"in fewer than {mp} minutes played")

    #print(" ".join(filter(None, lines)))

    # Combine all lines into a single string
    return " ".join(filter(None, lines))


# Example usage
#data_example = {'id': {'yabusgu01'}, 'name': {'Guerschon Yabusele'}, 'date': [(datetime.date(2025, 1, 21),)], 'stats': [{'stat_name': 'tov', 'operator': '<=', 'value': '0'}, {'stat_name': 'player_home_away', 'operator': '=', 'value': "'away'"}, {'stat_name': 'birth_country', 'operator': '=', 'value': "'France'"}, {'stat_name': 'efg_pct', 'operator': '>=', 'value': '0.75'}, {'stat_name': 'mp', 'operator': '<=', 'value': '25'}, {'stat_name': 'draft_round', 'operator': '=', 'value': "1"}, {'stat_name': 'fg', 'operator': '>=', 'value': '8'}]}
# clean_combo(data_example)

# id to player_id
#name to player_name
#date is good
#stats is good
#game_type is good
#team_id is good
