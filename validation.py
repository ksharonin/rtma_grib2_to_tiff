import os
import rasterio
import numpy as np
import pygrib
import re
from typing import Tuple, List
from pyproj import Transformer
from collections import Counter
import glob

def extract_timestamp(filename: str) -> int:
    try:
        # Match pattern tHHMMz e.g. rtma2p5_ru.t2000z.2dvaranl_ndfd.grb2
        match = re.search(r't(\d{2})(\d{2})z', filename)
        if match:
            hours, minutes = match.groups()
            return int(f"{hours}{minutes}")
        return 9999
    except Exception:
        return 9999

def create_coordinate_grid(tiff_path: str) -> Tuple[np.ndarray, np.ndarray]:

    # Lat/lon arrays per TIFF cell
    with rasterio.open(tiff_path) as src:
        height = src.height
        width = src.width
        rows, cols = np.mgrid[0:height, 0:width]
        
        # Affine transform
        transform = src.transform
        
        # Pixel coords to map coordinates
        x_coords = transform[2] + (cols + 0.5) * transform[0]
        y_coords = transform[5] + (rows + 0.5) * transform[4]
        
        return y_coords, x_coords
    
def get_fixed_sample_points(total_width: int, total_height: int, grid_size: int = 32) -> Tuple[np.ndarray, np.ndarray]:
    # Evenly spaced points along each dimension
    row_indices = np.linspace(0, total_height-1, grid_size, dtype=int)
    col_indices = np.linspace(0, total_width-1, grid_size, dtype=int)
    
    # Meshgrid of points
    rows, cols = np.meshgrid(row_indices, col_indices)
    
    return rows.flatten(), cols.flatten()

def validate_spatial_match(
    date: str,
    tiff_path: str,
    grib_directory: str,
    target_band_number: int,
    grid_size: int = 32  # Num points in each dimension
) -> bool:
    
    try:

        grib_files = glob.glob(os.path.join(grib_directory, "*.grb2"))
        grib_files = [entry for entry in grib_files if date in entry]
        grib_files.sort(key=lambda x: extract_timestamp(os.path.basename(x)))

        lats, lons = create_coordinate_grid(tiff_path)
        
        with rasterio.open(tiff_path) as tiff:
            num_bands = tiff.count
            if num_bands != len(grib_files):
                print(f"Band count mismatch: TIFF has {num_bands} bands, but found {len(grib_files)} GRIB files")
                return False
            
            sample_rows, sample_cols = get_fixed_sample_points(
                total_width=tiff.width,
                total_height=tiff.height,
                grid_size=grid_size
            )
            
            sample_size = len(sample_rows)
            print(f"Using {sample_size} fixed sample points ({grid_size}x{grid_size} grid)")
            
            sample_points = [(lats[row, col], lons[row, col]) 
                           for row, col in zip(sample_rows, sample_cols)]
            print("Sample points (lat, lon):")
            for i, (lat, lon) in enumerate(sample_points[:5]):
                print(f"Point {i+1}: ({lat:.4f}, {lon:.4f})")
            if len(sample_points) > 5:
                print("... and more points")
            
            for band_idx, grib_file in enumerate(grib_files, start=1):
                grib_path = os.path.join(grib_directory, grib_file)
                print(f"\nValidating band {band_idx} with file {grib_file}")
                
                tiff_data = tiff.read(band_idx)
                
                with pygrib.open(grib_path) as grbs:
                    grb = list(grbs)[target_band_number - 1]
                    
                    mismatches = 0
                    max_diff = 0
                    
                    for i in range(sample_size):
                        row, col = sample_rows[i], sample_cols[i]
                        lat, lon = lats[row, col], lons[row, col]
                        
                        tiff_value = tiff_data[row, col]
                        grib_value = grb.values[row, col]
                        
                        diff = abs(tiff_value - grib_value)
                        if diff > 1e-5:  # floating tolerance
                            mismatches += 1
                            max_diff = max(max_diff, diff)
                            
                            if mismatches <= 5:  # Log first 5 mismatches
                                print(f"Mismatch at lat={lat:.4f}, lon={lon:.4f}")
                                print(f"TIFF value: {tiff_value:.4f}, GRIB value: {grib_value:.4f}")
                    
                    # Report results for this band
                    if mismatches > 0:
                        print(f"\nFound {mismatches} mismatches out of {sample_size} points")
                        print(f"Maximum difference: {max_diff:.6f}")
                        return False
                    else:
                        print(f"All {sample_size} sample points match for band {band_idx}")
            
            print("\nAll bands validated successfully!")
            return True
            
    except Exception as e:
        print(f"Validation failed: {str(e)}")
        return False

def compare_value_distributions(grib_path: str, tiff_path: str, tiff_band: int, grib_band: int) -> bool:
    try:
        with rasterio.open(tiff_path) as tiff:
            tiff_data = tiff.read(tiff_band).flatten()
            
        with pygrib.open(grib_path) as grb:
            messages = list(grb)
            grib_data = messages[grib_band - 1].values.flatten()
            
        # Handle floating point precision
        tiff_data = np.round(tiff_data, decimals=6)
        grib_data = np.round(grib_data, decimals=6)
        
        tiff_counts = Counter(tiff_data)
        grib_counts = Counter(grib_data)
        
        tiff_values = set(tiff_counts.keys())
        grib_values = set(grib_counts.keys())
        
        if tiff_values != grib_values:
            print("Unique values don't match!\n")

            print(f"Values only in TIFF: {tiff_values - grib_values}\n")
            print(f"Values only in GRIB: {grib_values - tiff_values}\n")
            
            print(f"Number of values only in TIFF: {len(tiff_values - grib_values)}\n")
            print(f"Number of values only in GRIB: {len(grib_values - tiff_values)}\n")

            print(f"Total of values in TIFF: {len(tiff_values)}\n")
            print(f"Total of values in GRIB: {len(grib_values)}\n")
            
            return False
            
        for value in tiff_values:
            if tiff_counts[value] != grib_counts[value]:
                print(f"Count mismatch for value {value}:")
                print(f"TIFF count: {tiff_counts[value]}")
                print(f"GRIB count: {grib_counts[value]}")
                return False
                
        print("Success! Value distributions match exactly:")
        print(f"Number of unique values: {len(tiff_values)}")
        return True
        
    except Exception as e:
        print(f"Error during validation: {str(e)}")
        return False

def validate_band_values(date: str, grib_directory: str, tiff_path: str, grib_band: int) -> bool:

    try:
        grib_files = glob.glob(os.path.join(grib_directory, "*.grb2"))
        grib_files = [entry for entry in grib_files if date in entry]
        grib_files.sort(key=lambda x: extract_timestamp(os.path.basename(x)))
        
        with rasterio.open(tiff_path) as tiff:
            num_tiff_bands = tiff.count
            
        if num_tiff_bands != len(grib_files):
            print(f"Number of bands mismatch: TIFF has {num_tiff_bands}, found {len(grib_files)} GRIB files")
            return False
            
        for idx, grib_file in enumerate(grib_files, start=1):
            grib_path = os.path.join(grib_directory, grib_file)
            print(f"\nChecking band {idx} with file {grib_file}")
            
            if not compare_value_distributions(
                grib_path=grib_path,
                tiff_path=tiff_path,
                tiff_band=idx,
                grib_band=grib_band
            ):
                return False
                
        print("\nAll bands validated successfully!")
        return True
        
    except Exception as e:
        print(f"Validation failed: {str(e)}")
        return False