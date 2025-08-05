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
        df['player_id'] = df['player'].apply(lambda x: x.get("id") if isinstance(x,dict) else np.nan)
        df['player_name'] = df['player'].apply(lambda x: x.get("name") if isinstance(x,dict) else np.nan)

    if 'team' in df.columns:
        df['team_id'] = df['team'].apply(lambda x: x.get('id') if isinstance(x, dict) else np.nan)
        df['team_name'] = df['team'].apply(lambda x: x.get('name') if isinstance(x, dict) else np.nan)

    if 'position' in df.columns:
        df['position_id'] = df['position'].apply(lambda x: x.get('id') if isinstance(x, dict) else np.nan)
        df['position_name'] = df['position'].apply(lambda x: x.get('name') if isinstance(x, dict) else np.nan)

    if "location" in df.columns:
        df['location'] = df['location'].apply(lambda x: x if isinstance(x,list) else [np.nan, np.nan])
        df[['location_x', 'location_y']] = pd.DataFrame(df['location'].tolist(), index=df.index)

    if 'pass' in df.columns:
        df['pass.length'] = df['pass'].apply(lambda x: x.get('length') if isinstance(x,dict) else np.nan)
        df['pass.angle'] = df['pass'].apply(lambda x: x.get('angle') if isinstance(x,dict) else np.nan)
        df['pass.recepient_id'] = df['pass'].apply(lambda x: x.get('recipient', {}).get('id') if isinstance(x,dict) else np.nan)
        df['pass.recepient_name'] = df['pass'].apply(lambda x: x.get('recipient', {}).get('name') if isinstance(x,dict) else np.nan)
        df['pass.end_location'] = df['pass'].apply(lambda x: x.get('end_location') if isinstance(x,dict) else [np.nan, np.nan])
        df['pass.end_location'] = df['pass.end_location'].apply(lambda x: x if isinstance(x,list) else [np.nan, np.nan])
        df[['pass.end_location_x', 'pass.end_location_y']] = pd.DataFrame(df['pass.end_location'].tolist(), index=df.index)
   
    if 'carry' in df.columns:
        df['carry.length'] = df['carry'].apply(lambda x: x.get('length') if isinstance(x,dict) else np.nan)
        df['carry.angle'] = df['carry'].apply(lambda x: x.get('angle') if isinstance(x,dict) else np.nan)
        df['carry.recepient_id'] = df['carry'].apply(lambda x: x.get('recipient', {}).get('id') if isinstance(x,dict) else np.nan)
        df['carry.recepient_name'] = df['carry'].apply(lambda x: x.get('recipient', {}).get('name') if isinstance(x,dict) else np.nan)
        df['carry.end_location'] = df['carry'].apply(lambda x: x.get('end_location') if isinstance(x,dict) else [np.nan, np.nan])
        df['carry.end_location'] = df['carry.end_location'].apply(lambda x: x if isinstance(x,list) else [np.nan, np.nan])
        df[['carry.end_location_x', 'carry.end_location_y']] = pd.DataFrame(df['carry.end_location'].tolist(), index=df.index)
 
    if 'goalkeeper' in df.columns:
        df['goalkeeper.length'] = df['goalkeeper'].apply(lambda x: x.get('length') if isinstance(x,dict) else np.nan)
        df['goalkeeper.angle'] = df['goalkeeper'].apply(lambda x: x.get('angle') if isinstance(x,dict) else np.nan)
        df['goalkeeper.recepient_id'] = df['goalkeeper'].apply(lambda x: x.get('recipient', {}).get('id') if isinstance(x,dict) else np.nan)
        df['goalkeeper.recepient_name'] = df['goalkeeper'].apply(lambda x: x.get('recipient', {}).get('name') if isinstance(x,dict) else np.nan)
        df['goalkeeper.end_location'] = df['goalkeeper'].apply(lambda x: x.get('end_location') if isinstance(x,dict) else [np.nan, np.nan])
        df['goalkeeper.end_location'] = df['goalkeeper.end_location'].apply(lambda x: x if isinstance(x,list) else [np.nan, np.nan])
        df[['goalkeeper.end_location_x', 'goalkeeper.end_location_y']] = pd.DataFrame(df['goalkeeper.end_location'].tolist(), index=df.index)
    
    if 'shot' in df.columns:
        df['shot.length'] = df['shot'].apply(lambda x: x.get('length') if isinstance(x,dict) else np.nan)
        df['shot.angle'] = df['shot'].apply(lambda x: x.get('angle') if isinstance(x,dict) else np.nan)
        df['shot.end_location'] = df['shot'].apply(lambda x: x.get('end_location') if isinstance(x,dict) else [np.nan, np.nan])
        df['shot.end_location'] = df['shot.end_location'].apply(lambda x: x if isinstance(x,list) else [np.nan, np.nan])
        df[['shot.end_location_x', 'shot.end_location_y']] = pd.DataFrame(df['shot.end_location'].tolist(), index=df.index)

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
