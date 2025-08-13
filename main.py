import json
import pandas as pd 
import numpy as np
from collections import defaultdict
from metrics import compute_heatmap, _compute_channel_usage, _compute_third_usage, compute_windowed_metrics
from parser import load_events, parse_events, extract_tactics
from statsbombpy import sb
from scipy.sparse import coo_matrix, save_npz
from pathlib import Path
from datetime import datetime

match_id = 3942819
events_df = load_events(match_id=3942819)
events_df_final = parse_events(events_df)

tactics_df_final, main_df_final = extract_tactics(events_df_final)

heatmaps = compute_heatmap(main_df_final, by="team_name")

_compute_channel_usage(main_df_final, by="team_name")
insight_df_eng = main_df_final[
    (main_df_final['team_name'] == 'England') &
    (main_df_final['minute'] < 15)
].copy()

insight_df_net = main_df_final[
    (main_df_final['team_name'] == 'Netherlands') &
    (main_df_final['minute'] < 15)
].copy()

