import numpy as np
import pandas as pd
from collections import defaultdict

def compute_heatmap(df, by='team_name', grid_size=8):
    '''Function to compute heatmaps for given DataFrame. '''
    heatmaps = {}
    xedges = np.linspace(0,100, grid_size+1)
    yedges = np.linspace(0,100, grid_size+1)
    for key, group in df.groupby(by):
        xs = group["location_x"].dropna()
        ys = group["location_y"].dropna()
        heat, _, _ = np.histogram2d(xs, ys, bins=[xedges, yedges])
        heatmaps[key] = heat
    return heatmaps

def assign_channel(x):
    '''Function to assign a channel based on x-coordinate.'''
    if x <33.33:
        return "left"
    elif x<66.66:
        return "middle"
    else:
        return "right"
    
def assign_third(y):
    '''Function to assign a third based on y-coordinate.'''
    if y<33.33:
        return "defensive"
    if y<66.66:
        return "middle"
    else:
        return "final"

def _compute_channel_usage(df, by="team_name"):
    '''Function to compute channel usage based on x-coordinate.'''
    df = df.copy()
    df["channel"] = df["location_x"].apply(assign_channel)
    return df.groupby(["channel", by]).size().unstack(fill_value=0)

def _compute_third_usage(df, by="team_name"):
    '''Function to compute third usage based on y-coordinate.'''
    df = df.copy()
    df["third"] = df["location_y"].apply(assign_third)
    return df.groupby(["third", by]).size().unstack(fill_value=0)    

def _calculate_windowed_pass_flux(df_pass, grid_size=8):
    xbins = np.linspace(0, 100, grid_size + 1)
    ybins = np.linspace(0, 100, grid_size + 1)
    
    startsx = np.digitize(df_pass["location_x"], xbins) - 1
    startsy = np.digitize(df_pass["location_y"], ybins) - 1
    endsx = np.digitize(df_pass["pass_end_location_x"], xbins) - 1
    endsy = np.digitize(df_pass["pass_end_location_y"], ybins) - 1

    starts_flat = startsx * grid_size + startsy
    ends_flat = endsx * grid_size + endsy

    max_index = grid_size * grid_size
    mask = (starts_flat >= 0) & (starts_flat < max_index) & (ends_flat >= 0) & (ends_flat < max_index)
    starts_filtered = starts_flat[mask]
    ends_filtered = ends_flat[mask]

    flux = np.zeros((max_index, max_index), dtype=int)

    np.add.at(flux, (starts_filtered, ends_filtered), 1)

    return flux

def compute_windowed_metrics(df, window_size=15, grid_size=8):
    '''Function to compute windowed pass fluxes.'''
    windowed_pass_fluxes = defaultdict(dict)
    dfp = df[df["type.name"] == "Pass"].dropna(subset=["location_x", "location_y", "pass_end_location_x", "pass_end_location_y"]).copy()
    dfp["time_block"] = dfp["minute"] // window_size

    for (team_name, block_index), group in dfp.groupby(["team_name", "time_block"]):
        flux = _calculate_windowed_pass_flux(group, grid_size)
        channel = _compute_channel_usage(group)
        third = _compute_third_usage(group)

        start_minute = block_index * window_size
        end_minute = start_minute + window_size - 1

        windowed_pass_fluxes[team_name][block_index] = {
            "flux": flux,
            "channel": channel,
            "third": third,
            "window": (start_minute, end_minute)
        }

    return windowed_pass_fluxes

def df_with_channel_and_third(df):
    '''Function to add channel and third columns to DataFrame.'''
    df_with_channel_and_third = df.copy()
    df_with_channel_and_third["channel"] = df_with_channel_and_third["location_x"].apply(assign_channel)
    df_with_channel_and_third["third"] = df_with_channel_and_third["location_y"].apply(assign_third)
    return df_with_channel_and_third

def heatmap_minute(df, grid_size=8):
    '''Function to compute heatmap by minute with channel and third.'''
    df_with_channel_and_third = df_with_channel_and_third(df)
    heatmap_by_minute = df_with_channel_and_third.groupby(['team_name', 'minute', 'channel', 'third']).size().unstack(fill_value=0)
    return heatmap_by_minute