import json
import pandas as pd 
import numpy as np

def load_json(file_path):
    '''Function to load JSON data from a file.'''
    with open(file_path, "r") as file:
        data = json.load(file)
    return data

def parse_events(json_data):
    '''Function to parse events from JSON data into a DataFrame.'''
    df = pd.json_normalize(json_data, sep=".")
    df["pass.length"] = df["pass.length"].astype(float)
    df["pass.angle"] = df["pass.angle"].astype(float)
    df["location"] = df["location"].apply(lambda x: x if isinstance(x,list) else [np.nan, np.nan])
    df[["location_x", "location_y"]] = pd.DataFrame(df["location"].tolist(), index=df.index)
    df["pass.end_location"] = df["pass.end_location"].apply(lambda x: x if isinstance(x,list) else [np.nan, np.nan])
    df[["pass.end_location_x", "pass.end_location_y"]] = pd.DataFrame(df["pass.end_location"].tolist(), index=df.index)
    df["carry.end_location"] = df["carry.end_location"].apply(lambda x: x if isinstance(x,list) else [np.nan, np.nan])
    df[["carry.end_location_x", "carry.end_location_y"]] = pd.DataFrame(df["carry.end_location"].tolist(), index=df.index)
    df["shot.end_location"] = df["shot.end_location"].apply(
    lambda x: x if isinstance(x, list) and len(x) == 2 else [np.nan, np.nan]
    )
    df[["shot.end_location_x", "shot.end_location_y"]] = pd.DataFrame(
        df["shot.end_location"].tolist(), index=df.index
    )
    df["goalkeeper.end_location"] = df["goalkeeper.end_location"].apply(lambda x: x if isinstance(x,list) else [np.nan, np.nan])
    df[["goalkeeper.end_location_x", "goalkeeper.end_location_y"]] = pd.DataFrame(df["goalkeeper.end_location"].tolist(), index=df.index)
    return df

def extract_tactics(df):
    '''Function to extract tactics-related events from the DataFrame.'''
    mask = df["type.name"].isin(["Starting XI"])
    starting11_df = df[mask]
    starting11_df["lineup"] = starting11_df["tactics.lineup"].copy()
    lineup_df = starting11_df.explode("lineup").reset_index(drop=True)
    valid_lineup = [x for x in lineup_df["lineup"] if isinstance(x, dict)]
    player_metadata = pd.json_normalize(valid_lineup, sep=".", meta=["possession_team.name"])
    subs_df = df[df["type.name"] == "Substitution"]
    tactical_events = df[df["type.name"].isin(["Starting XI", "Tactical Shift", "Substitution"])].copy()
    team_states = [] 
    current_lineups = {}

    for _, event in tactical_events.iterrows():
        if event["type.name"] == "Starting XI":
            team_id = event["team.id"]
            current_lineups[team_id] = {
            "formation": event["tactics.formation"], 
            "players": [{
                "player_id": p["player"]["id"],
                "player_name": p["player"]["name"],
                "position_id": p["position"]["id"],
                "position_name": p["position"]["name"],
                "jersey_number": p["jersey_number"]
            }
            for p in event["tactics.lineup"]
            ]
            }
            team_states.append({
                'team_id': team_id, 
                'team_name':event["team.name"],
                'minute': event['minute'],
                "second": event['second'], 
                'event_type': "Starting XI", 
                "formation": event["tactics.formation"], 
                "lineup": current_lineups[team_id]["players"],
                'substituted_in': None, 
                'substituted_out': None

            })

        elif event["type.name"] == "Substitution":
            team_id = event["team.id"]
            sub_player_id = event['player.id']
            incoming_player_id = event['substitution.replacement.id']
            incoming_player_name = event['substitution.replacement.name']
            lineup = current_lineups[team_id]["players"]
            outgoing_player = None
            for i, p in enumerate(lineup):
                if p["player_id"] == sub_player_id:
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
                'team_name': event["team.name"],
                'minute': event['minute'], 
                'second': event['second'],
                'event_type': "Substitution",
                "formation": current_lineups[team_id]["formation"],
                "lineup": current_lineups[team_id]["players"],
                "substituted_in": incoming_player_id,
                "substituted_out": sub_player_id
            })
    
        elif event["type.name"] == "Tactical Shift":
            team_id = event["team.id"]
            current_lineups[team_id] = {
                'formation': event["tactics.formation"],
                "players": [{
                    "player_id": p["player"]["id"],
                    "player_name": p["player"]["name"],
                    "position_id": p["position"]["id"],
                    "position_name": p["position"]["name"],
                    "jersey_number": p["jersey_number"]
                } for p in event["tactics.lineup"]
                ]
            }   

            team_states.append({
            'team_id': team_id,
            'team_name': event['team.name'],
            'minute': event['minute'],
            'second': event['second'],
            'event_type': 'Tactical Shift',
            'formation': event['tactics.formation'],
            'lineup': current_lineups[team_id]['players'].copy(),
            'substituted_in': None,  
            'substituted_out': None
        })
            
    df_lineups = pd.DataFrame(team_states)
    df_players = df_lineups.explode("lineup").reset_index(drop=True)
    player_data = df_players["lineup"].apply(pd.Series)
    df_tactics_final = pd.concat([df_players.drop(columns=["lineup"]), player_data], axis=1)
    df_main_final = df[~df["type.name"].isin(["Starting XI", "Tactical Shift", "Substitution"])]
    return df_tactics_final, df_main_final
