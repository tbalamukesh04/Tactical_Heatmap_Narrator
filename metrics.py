import numpy as np
import pandas as pd

def compute_heatmap(df, by='team.name', grid_size=8):
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

def compute_channel_usage(df, by="team.name"):
    '''Function to compute channel usage based on x-coordinate.'''
    df = df.copy()
    df["channel"] = df["location_x"].apply(assign_channel)
    return df.groupby(["channel", by]).size().unstack(fill_value=0)

def compute_third_usage(df, by="team.name"):
    '''Function to compute third usage based on y-coordinate.'''
    df = df.copy()
    df["third"] = df["location_y"].apply(assign_third)
    return df.groupby(["third", by]).size().unstack(fill_value=0)    

def compute_pass_flux(df, grid_size=8):
    '''Function to compute pass flux between grid cells.'''
    dfp = df[df["type.name"] == "Pass"].dropna(subset=["location_x", "location_y", "pass.end_location_x", "pass.end_location_y"])
    xbins = np.linspace(0, 100, grid_size + 1)
    ybins = np.linspace(0, 100, grid_size + 1)
    startsx = np.digitize(dfp["location_x"], xbins) - 1
    startsy = np.digitize(dfp["location_y"], ybins) - 1
    endsx = np.digitize(dfp["pass.end_location_x"], xbins) - 1
    endsy = np.digitize(dfp["pass.end_location_y"], ybins) - 1

    starts_flat = startsx * grid_size + startsy
    ends_flat = endsx * grid_size + endsy

    flux = np.zeros((grid_size*grid_size, grid_size*grid_size), dtype=int)
    for i, j in zip(starts_flat, ends_flat):
        if 0<=i < grid_size*grid_size and 0<=j<grid_size*grid_size:
            flux[i,j] += 1

    return flux

def df_with_channel_and_third(df):
    '''Function to add channel and third columns to DataFrame.'''
    df_with_channel_and_third = df.copy()
    df_with_channel_and_third["channel"] = df_with_channel_and_third["location_x"].apply(assign_channel)
    df_with_channel_and_third["third"] = df_with_channel_and_third["location_y"].apply(assign_third)
    return df_with_channel_and_third

def heatmap_minute(df, grid_size=8):
    '''Function to compute heatmap by minute with channel and third.'''
    df_with_channel_and_third = df_with_channel_and_third(df)
    heatmap_by_minute = df_with_channel_and_third.groupby(['team.name', 'minute', 'channel', 'third']).size().unstack(fill_value=0)
    return heatmap_by_minute