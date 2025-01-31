import stat_combo_clean
import datetime

data_for_input = {'id': {'gueyemo02'}, 
                  'name': {'Mouhamed Gueye'}, 
                  'date': [(datetime.date(2025, 1, 28),)], 
                  'game_id': [('GSWUTA20250128',)], 
                  'stats': [{'stat_name': 'last_name', 'operator': '=', 'value': "'Gueye'"}, 
                            {'stat_name': 'fg', 'operator': '>=', 'value': '3'}, 
                            {'stat_name': 'mp', 'operator': '<=', 'value': '25'}], 
                            'game_type': 'Regular Season'}

result = stat_combo_clean.clean_combo(data_for_input)