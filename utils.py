import numpy as np
from scipy.sparse import coo_matrix, save_npz
from pathlib import Path
import pandas as pd
import json
from datetime import datetime

def _calculate_flux_matrix(df_pass, grid_size=8):
    xbins = np.linspace(0, 100, grid_size+1)
    ybins = np.linspace(0, 100, grid_size+1)

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

    data = np.ones(len(starts_filtered), dtype=int)
    flux_coo = coo_matrix((data, (starts_filtered, ends_filtered)), shape=(max_index, max_index))
    flux_csr = flux_coo.tocsr()

    return flux_csr

def _calculate_flux_matrix(df_pass, grid_size=8):
    xbins = np.linspace(0, 100, grid_size+1)
    ybins = np.linspace(0, 100, grid_size+1)

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

    data = np.ones(len(starts_filtered), dtype=int)
    flux_coo = coo_matrix((data, (starts_filtered, ends_filtered)), shape=(max_index, max_index))
    flux_csr = flux_coo.tocsr()

    return flux_csr
def save_match_metrics(windowed_metrics, match_id, team_infor, output_dir):
    base_path = Path(output_dir)
    metrics_path = base_path / "metrics"
    flux_path = metrics_path / "flux_matrices"
    flux_path.mkdir(parents=True, exist_ok=True)

    all_metrics_list = []

    for team_name, blocks in windowed_metrics.items():
        team_id = team_infor.get(team_name)
        for block_index, metrics_data in blocks.items():
            window = metrics_data['window']
            start_min, end_min = int(window[0]), int(window[1])
            flux_matrix = metrics_data.get('flux')
            if flux_matrix is not None and flux_matrix.nnz > 0:
                flux_filename = f"{match_id}_{team_id}_{start_min}-{end_min}_flux.npz"
                flux_filepath = flux_path / flux_filename

                save_npz(flux_filepath, flux_matrix)
            
            channel_series = metrics_data.get('channel')
            if isinstance(channel_series, pd.Series):
                channel_counts = channel_series.reset_index().rename(columns={channel_series.name: 'count'}).to_dict('records')

            elif isinstance(channel_series, pd.DataFrame):
                count_channel_name = [col for col in channel_series.columns if col != 'channel'][0]
                channel_counts = channel_series.reset_index().rename(columns={count_channel_name: 'count'}).to_dict('records')

            else:
                channel_counts = []
                
            third_series = metrics_data.get('third')
            if isinstance(third_series, pd.Series):
                third_counts = third_series.reset_index().rename(columns={third_series.name: 'count'}).to_dict('records')

            elif isinstance(third_series, pd.DataFrame):
                count_third_name = [col for col in third_series.columns if col != 'third'][0]
                third_counts = third_series.reset_index().rename(columns={count_third_name: 'count'}).to_dict('records')

            else:
                third_counts = []

            flux_metric_obj = {
                "metric_type": "pass_flux_window_matrix", 
                "team_id": team_id, 
                "time_window": {"start_minute": start_min, 
                                "end_minute": end_min},
                "grid_size": flux_matrix.shape[0] ** 0.5, 
                "flux_matrix_path": str(flux_filepath.relative_to(base_path)), 
                "cell_indexing": "row_major: (row*grid_size + col)"
            }

            zone_usage_obj = {
                    "metric_type": "zone_usage",
                    "team_id": team_id,
                    "event_type": "Pass", 
                    "time_window": {"start_minute": start_min, "end_minute": end_min},
                    "channel_counts": channel_counts,
                    "third_counts": third_counts
                }
                
            all_metrics_list.append(flux_metric_obj)
            all_metrics_list.append(zone_usage_obj)
    
    final_output = {
        "match_id": match_id, 
        'teams': [{'team_id': v, 'team_name': k} for k, v in team_infor.items()], 
        'metrics_generated_at': datetime.now().isoformat(), 
        'metrics': all_metrics_list
    }

    json_filepath = metrics_path / f"{match_id}_metrics.json"
    with open(json_filepath, 'w') as f:
        json.dump(final_output, f, indent=2)

    print(f'Successfully saved metrics to {json_filepath}')

def save_match_metrics(windowed_metrics, match_id, team_infor, output_dir):
    base_path = Path(output_dir)
    metrics_path = base_path / "metrics"
    flux_path = metrics_path / "flux_matrices"
    flux_path.mkdir(parents=True, exist_ok=True)

    all_metrics_list = []

    for team_name, blocks in windowed_metrics.items():
        team_id = team_infor.get(team_name)
        for block_index, metrics_data in blocks.items():
            window = metrics_data['window']
            start_min, end_min = int(window[0]), int(window[1])
            flux_matrix = metrics_data.get('flux')
            if flux_matrix is not None and flux_matrix.nnz > 0:
                flux_filename = f"{match_id}_{team_id}_{start_min}-{end_min}_flux.npz"
                flux_filepath = flux_path / flux_filename

                save_npz(flux_filepath, flux_matrix)
            
            channel_series = metrics_data.get('channel')
            if isinstance(channel_series, pd.Series):
                channel_counts = channel_series.reset_index().rename(columns={channel_series.name: 'count'}).to_dict('records')

            elif isinstance(channel_series, pd.DataFrame):
                count_channel_name = [col for col in channel_series.columns if col != 'channel'][0]
                channel_counts = channel_series.reset_index().rename(columns={count_channel_name: 'count'}).to_dict('records')

            else:
                channel_counts = []
                
            third_series = metrics_data.get('third')
            if isinstance(third_series, pd.Series):
                third_counts = third_series.reset_index().rename(columns={third_series.name: 'count'}).to_dict('records')

            elif isinstance(third_series, pd.DataFrame):
                count_third_name = [col for col in third_series.columns if col != 'third'][0]
                third_counts = third_series.reset_index().rename(columns={count_third_name: 'count'}).to_dict('records')

            else:
                third_counts = []

            flux_metric_obj = {
                "metric_type": "pass_flux_window_matrix", 
                "team_id": team_id, 
                "time_window": {"start_minute": start_min, 
                                "end_minute": end_min},
                "grid_size": flux_matrix.shape[0] ** 0.5, 
                "flux_matrix_path": str(flux_filepath.relative_to(base_path)), 
                "cell_indexing": "row_major: (row*grid_size + col)"
            }

            zone_usage_obj = {
                    "metric_type": "zone_usage",
                    "team_id": team_id,
                    "event_type": "Pass", 
                    "time_window": {"start_minute": start_min, "end_minute": end_min},
                    "channel_counts": channel_counts,
                    "third_counts": third_counts
                }
                
            all_metrics_list.append(flux_metric_obj)
            all_metrics_list.append(zone_usage_obj)
    
    final_output = {
        "match_id": match_id, 
        'teams': [{'team_id': v, 'team_name': k} for k, v in team_infor.items()], 
        'metrics_generated_at': datetime.now().isoformat(), 
        'metrics': all_metrics_list
    }

    json_filepath = metrics_path / f"{match_id}_metrics.json"
    with open(json_filepath, 'w') as f:
        json.dump(final_output, f, indent=2)

    print(f'Successfully saved metrics to {json_filepath}')

    
            
def summarize_flux_matrix(flux_csr_matrix, top_n_lanes=3):
    if flux_csr_matrix is None or flux_csr_matrix.nnz == 0:
        return {"total_passes": 0, "top_lanes": []}
    
    flux_coo_matrix = flux_csr_matrix.tocoo()
    
    total_passes = int(flux_coo_matrix.nnz)
    pass_lanes = sorted(zip(flux_coo_matrix.data, flux_coo_matrix.row, flux_coo_matrix.col), 
                        key=lambda x: x[0], reverse=True)
    top_lanes = []
    for count, from_cell, to_cell in pass_lanes[:top_n_lanes]:
        top_lanes.append({
            "from_cell" : int(from_cell), 
            "to_cell": int(to_cell), 
            "count": int(count)
        })

        return {
            "total_passes": total_passes, 
            'top_lanes': top_lanes
        }