#!/usr/bin/env python3.13

import rasterio
import glob
import os
from osgeo import gdal
import cfgrib
import boto3
import pygrib
from botocore import UNSIGNED
from botocore.config import Config
from validation import validate_band_values, validate_spatial_match, extract_timestamp

# Suppress PROJ warning
os.environ['PROJ_LIB'] = '/opt/homebrew/share/proj'

# Settings
DATE = "20250108" # Format as: YYYYMMDD
TIME_STEP_MINUTES = 60 # starting at 0000, control timestep of band sampling
# Band 8: WindDir, Band 9: WindSpeed 
BAND_NUMBER = 9

# e.g. Want to target file(s) in s3://noaa-rtma-pds/rtma2p5_ru.20250107/rtma2p5_ru.t0000z.2dvaranl_ndfd.grb2 
AWS_BUCKET_NAME = "noaa-rtma-pds" # See documentation https://registry.opendata.aws/noaa-rtma/
AWS_PREFIX = "rtma2p5_ru"
AWS_POSTFIX = "2dvaranl_ndfd.grb2"

DEBUG_DOWNLOADED_FILES = True # keeps downloaded files from AWS, otherwise delete when done

# TODO 
# refactor to use dynamic paths picked up from script
DOWNLOAD_LOCATION_PATH = "/Users/katrinasharonin/Downloads/firelab_work/grib2_to_tiff/input/"
OUTPUT_DIR_PATH = "/Users/katrinasharonin/Downloads/firelab_work/grib2_to_tiff/output/"
OUTPUT_FILE_PATH = f"/Users/katrinasharonin/Downloads/firelab_work/grib2_to_tiff/output/{DATE}_ws_merged.tiff"


def read_grib2_band(
    grib2_file_path: str,
    band_name: str,
):
    grib_data = cfgrib.open_dataset(grib2_file_path)
    return grib_data[band_name]

def merge_bands(
        date: str,
        directory_path: str, 
        output_file_path: str, 
        target_band_number: int
    ):

    print(f"Starting band merge process for band {target_band_number}")
    
    try:
        grib_files = glob.glob(os.path.join(directory_path, "*.grb2"))
        grib_files = [entry for entry in grib_files if date in entry]
        if not grib_files:
            print(f"No .grb2 files found in {directory_path} with matching date")
            return
            
        # Sort files for earliest timestamp first
        grib_files.sort(key=lambda x: extract_timestamp(os.path.basename(x)))
        
        with open(output_file_path, 'wb') as outfile:
            for grib_path in grib_files:
                try:
                    pygrib.tolerate_badgrib_on()

                    with pygrib.open(grib_path) as grbs:

                        messages = list(grbs)
                        
                        if target_band_number > len(messages):
                            print(f"Target band {target_band_number} exceeds number of bands in {grib_path}")
                            continue
                            
                        # Fetch band, -1 due to 0 indexing in python
                        target_msg = messages[target_band_number - 1]
                        
                        if target_msg is None:
                            print(f"Could not read band {target_band_number} from {grib_path}")
                            continue
                            
                        outfile.write(target_msg.tostring())
                        time = extract_timestamp(os.path.basename(grib_path))
                        print(f"Successfully processed band {target_band_number} from timestamp {time:04d}")
                        
                except Exception as e:
                    print(f"Error processing file {grib_path}: {str(e)}")
                    continue
                    
        return os.path.exists(output_file_path) and os.path.getsize(output_file_path) > 0
        
    except Exception as e:
        print(f"Fatal error in merge_bands: {str(e)}")
        return False

def download_from_s3(
    bucket_name: str,
    s3_object: str,
    output_file_path: str,
):
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    s3.download_file(
        Bucket=bucket_name, 
        Key=s3_object, 
        Filename=output_file_path
    )

def download_grib_files(
    num_keys: int,
    master_key: str,
    date: str,
    time_step: int,
    output_directory: str,
):
    print("Begin GRIB2 downloads for date: %s with timestep (minutes): %s" % (date, time_step))
    # Form keys based on time step, assumes each dir spans 24 hrs
    current = 0

    for _ in range(num_keys):
        # Extract hours and minutes, increment, reconvert with padding
        hours = current // 100
        minutes = current % 100

        if hours >= 24:
            print("\033[31mIllegal new_hours value, cannot be 24+ and got %i \033[0m" % new_hours)
            break
        if minutes >= 60:
            print("\033[31mIllegal new_minutes value, cannot be 60+ and got %i \033[0m" % new_minutes)
            break

        output_path = output_directory + date + "_" + AWS_PREFIX + ".t" + f"{current:04d}" + "z." + AWS_POSTFIX
        key = master_key + "/" + AWS_PREFIX + ".t" + f"{current:04d}" + "z." + AWS_POSTFIX

        print("Attempting to download file key %s from bucket %s" % (key, AWS_BUCKET_NAME))

        # Download file
        if os.path.exists(output_path):
            print("\033[33mSkipping download. File already exists at path: %s\033[0m" % output_path)
        else: 
            download_from_s3(
                bucket_name=AWS_BUCKET_NAME,
                s3_object=key,
                output_file_path=output_path,
            )
            print("\033[32mSuccessfully downloaded to output path: %s \033[0m" % output_path)

        # Increment to next timestamp
        total_minutes = hours * 60 + minutes + time_step
        new_hours = total_minutes // 60
        new_minutes = total_minutes % 60
        current = new_hours * 100 + new_minutes

def grib2_to_tiff(
    input_file_path: str,
    output_file_path: str
):
    print("\nConverting GRIB2 file at %s into TIFF...\n" % input_file_path)

    src_ds = gdal.Open(input_file_path)
    assert src_ds, "Failed to open grib2 file from path: %s" % input_file_path

    # Pick up bands
    bands = []
    bandNum = src_ds.RasterCount
    for i in range(1, bandNum+1): # GDAL starts band counts at 1
        bands.append(i)

    # Convert to TIFF
    gdal.Translate(output_file_path, src_ds, format="GTiff", bandList=bands)

    # Verify all bands captured
    input_num_bands = gdal.Open(input_file_path).RasterCount
    output_num_bands = gdal.Open(output_file_path).RasterCount
    assert input_num_bands == output_num_bands, "Got mismatching band count: saw %i bands in GRIB2, got %i bands in TIFF" % (input_num_bands, output_num_bands)

    print("\nDone! Check %s for output TIFF\n" % output_file_path)
    
def main():

    # Set up directories
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(script_dir, "input")
    if not os.path.exists(input_path):
        os.makedirs(input_path)
    output_path = os.path.join(script_dir, "output")
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # Download targeted GRIB files
    master_key = AWS_PREFIX + "." + DATE
    num_keys = (60*24 // TIME_STEP_MINUTES) # since 2400 does not exist in dir, 0000-2359 is the valid time range
    download_grib_files(
        num_keys=num_keys, 
        master_key=master_key, 
        date=DATE,
        time_step=TIME_STEP_MINUTES,
        output_directory=DOWNLOAD_LOCATION_PATH,
    )

    num_files = len([entry for entry in os.listdir(DOWNLOAD_LOCATION_PATH) if os.path.isfile(os.path.join(DOWNLOAD_LOCATION_PATH, entry)) and ".grb2" in entry and DATE in entry])
    assert num_keys == num_files, "Invalid number of downloaded files: expected %i, got %i" % (num_keys, num_files)

    # Extract and consolidate all bands into new GRIB
    merged_file_name = OUTPUT_DIR_PATH + DATE + "_BAND_" + str(BAND_NUMBER) + "_MERGED.grb2"
    merge_bands(
        date=DATE,
        directory_path=DOWNLOAD_LOCATION_PATH,
        output_file_path=merged_file_name,
        target_band_number=BAND_NUMBER,
    )

    # Convert merged grib into tiff
    grib2_to_tiff(input_file_path=merged_file_name, output_file_path=OUTPUT_FILE_PATH)

    # Verification
    success = validate_band_values(
        date=DATE,
        grib_directory=DOWNLOAD_LOCATION_PATH,
        tiff_path=OUTPUT_FILE_PATH,
        grib_band=BAND_NUMBER
    )

    success = validate_spatial_match(
        date=DATE,
        tiff_path=OUTPUT_FILE_PATH,
        grib_directory=DOWNLOAD_LOCATION_PATH,
        target_band_number=BAND_NUMBER
    )

    assert success, "Failed to verify contents of TIFF"

    return 0

if __name__ == "__main__":
    main()