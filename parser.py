from statsbombpy import sb
import pandas as pd 
import numpy as np

def load_events(match_id):
    events_df = sb.events(match_id=match_id)
    return events_df

def parse_events(events_df):
    '''Function to parse events from JSON data into a DataFrame.'''
    df = events_df.copy()
    if "player" in df.columns:
        df['player_name'] = df['player']

    if 'team' in df.columns:
        df['team_name'] = df['team']

    if 'position' in df.columns:
        df['position_id'] = df['position'].apply(lambda x: x.get('id') if isinstance(x, dict) else np.nan)
        df['position_name'] = df['position'].apply(lambda x: x.get('name') if isinstance(x, dict) else np.nan)

    if "location" in df.columns:
        df['location'] = df['location'].apply(lambda x: x if isinstance(x, list) else [np.nan, np.nan])
        locations = df['location'].apply(pd.Series)
        locations.columns = ['location_x', 'location_y']
        df = pd.concat([df, locations], axis=1)

    if 'pass_end_location' in df.columns:
        df['pass_end_location'] = df['pass_end_location'].apply(lambda x: x if isinstance(x, list) else [np.nan, np.nan])
        pass_end_locs = df['pass_end_location'].apply(pd.Series)
        pass_end_locs.columns = ['pass_end_location_x', 'pass_end_location_y']
        df = pd.concat([df, pass_end_locs], axis=1)
   
    if 'carry_end_location' in df.columns:
        df['carry_end_location'] = df['carry_end_location'].apply(lambda x: x if isinstance(x, list) else [np.nan, np.nan])
        carry_end_locs = df['carry_end_location'].apply(pd.Series)
        carry_end_locs.columns = ['carry_end_location_x', 'carry_end_location_y']
        df = pd.concat([df, carry_end_locs], axis=1)
 
    if 'goalkeeper_end_location' in df.columns:
        df['goalkeeper_end_location'] = df['goalkeeper_end_location'].apply(lambda x: x if isinstance(x, list) else [np.nan, np.nan])
        gk_end_locs = df['goalkeeper_end_location'].apply(pd.Series)
        gk_end_locs.columns = ['goalkeeper_end_location_x', 'goalkeeper_end_location_y']
        df = pd.concat([df, gk_end_locs], axis=1)
    
    if 'shot_end_location' in df.columns:
        df['shot_end_location'] = df['shot_end_location'].apply(lambda x: x if isinstance(x, list) else [np.nan, np.nan])
        shot_end_locs = df['shot_end_location'].apply(pd.Series)
        shot_end_locs.columns = ['shot_end_location_x', 'shot_end_location_y', 'shot_end_location_z']
        df = pd.concat([df, shot_end_locs], axis=1)

    if 'type' in df.columns and 'type.name' not in df.columns:
        df.rename(columns={'type': 'type.name'}, inplace=True)

    return df

def extract_tactics(df):
    '''Function to extract tactics-related events from the DataFrame.'''
    tactical_events = df[df['type.name'].isin(["Starting XI", "Substitution", "Tactical Shift"])].copy()
    team_states = []
    current_lineups = {}
    for _, event in tactical_events.iterrows():
            if event["type.name"] == "Starting XI":
                team_id = event["team"]
                current_lineups[team_id] = {
                "formation": event['tactics'].get('formation'), 
                "players": [{
                    "player_id": p["player"]["id"],
                    "player_name": p["player"]["name"],
                    "position_id": p["position"]["id"],
                    "position_name": p["position"]["name"],
                    "jersey_number": p["jersey_number"]
                }
                for p in event["tactics"].get('lineup')
                ]
                }
                team_states.append({
                    'team_id': team_id, 
                    'team_name':event["team_name"],
                    'minute': event['minute'],
                    "second": event['second'], 
                    'event_type': "Starting XI", 
                    "formation": event["tactics"].get('formation'), 
                    "lineup": current_lineups[team_id]["players"],
                    'substituted_in': None, 
                    'substituted_out': None

                })
        
        
            elif event["type.name"] == "Substitution":
                team_id = event["team"]
                sub_player_id = event['player']
                incoming_player_id = event['substitution_replacement_id']
                incoming_player_name = event['substitution_replacement']
                lineup = current_lineups.get(team_id).get("players")
                outgoing_player = None
                for i, p in enumerate(lineup):
                    if p["player_name"] == sub_player_id:
                        outgoing_player = p
                        lineup[i] = {
                            'player_id': incoming_player_id, 
                            'player_name': incoming_player_name, 
                            'position_id': outgoing_player["position_id"],
                            'position_name': outgoing_player["position_name"],
                            'jersey_number': None
                        }
                        break

                team_states.append({
                    'team_id': team_id, 
                    'team_name': event["team_name"],
                    'minute': event['minute'], 
                    'second': event['second'],
                    'event_type': "Substitution",
                    "formation": current_lineups[team_id]["formation"],
                    "lineup": current_lineups[team_id]["players"],
                    "substituted_in": incoming_player_name,
                    "substituted_out": sub_player_id
                })  
    
            elif event["type.name"] == "Tactical Shift":
                team_id = event["team"]
                current_lineups[team_id] = {
                    'formation': event["tactics"].get('formation'),
                    "players": [{
                        "player_id": p["player"]["id"],
                        "player_name": p["player"]["name"],
                        "position_id": p["position"]["id"],
                        "position_name": p["position"]["name"],
                        "jersey_number": p["jersey_number"]
                    } for p in event["tactics"].get("lineup")
                    ]
                }   

                team_states.append({
                'team_id': team_id,
                'team_name': event['team_name'],
                'minute': event['minute'],
                'second': event['second'],
                'event_type': 'Tactical Shift',
                'formation': event['tactics'].get('formation'),
                'lineup': current_lineups[team_id]['players'].copy(),
                'substituted_in': None,  
                'substituted_out': None
            })

    df_lineups = pd.DataFrame(team_states)
    if not df_lineups.empty:
        df_lineups = df_lineups.explode('lineup').reset_index(drop=True)
        player_data = df_lineups['lineup'].apply(pd.Series)
        df_tactics_final = pd.concat([df_lineups.drop(columns=['lineup']), player_data], axis=1)

    else:
        df_tactics_final = pd.DataFrame()

    df_main_final = df[~df["type.name"].isin(["Starting XI", "Substitution", "Tactical Shift"])].copy()

    return df_tactics_final, df_main_final
