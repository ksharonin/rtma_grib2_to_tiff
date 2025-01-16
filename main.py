#!/usr/bin/env python3.13

import traceback
import os
from osgeo import gdal
import cfgrib
import boto3
import pygrib
from botocore import UNSIGNED
from botocore.config import Config

# Suppress PROJ warning
os.environ['PROJ_LIB'] = '/opt/homebrew/share/proj'

# Settings
DATE = "20250107" # Format as: YYYYMMDD
TIME_STEP_MINUTES = 60 # starting at 0000, control timestep of band sampling
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
OUTPUT_FILE_PATH = "/Users/katrinasharonin/Downloads/firelab_work/grib2_to_tiff/output/rtma2p5_ru.t2245z.2dvaranl_ndfd.tiff"


def read_grib2_band(
    grib2_file_path: str,
    band_name: str,
):
    grib_data = cfgrib.open_dataset(grib2_file_path)
    return grib_data[band_name]

def merge_bands(
    directory_path: str,
    output_file_path: str,
    target_band_number: int,
):
    """
    Merge specific bands from multiple GRIB2 files with enhanced error handling and debugging.
    
    Args:
        directory_path: Path to directory containing GRIB2 files
        output_file_path: Path for output merged file
        target_band_number: The band number to extract from each file
    """
    input_files = [
        os.path.join(directory_path, entry)
        for entry in os.listdir(directory_path)
        if os.path.isfile(os.path.join(directory_path, entry))
        and entry.endswith('.grb2')
    ]
    
    if not input_files:
        raise ValueError(f"No .grb2 files found in {directory_path}")
    
    try:
        messages = []
        for file_path in input_files:
            try:
                grbs = pygrib.open(file_path)
                
                print(f"\nProcessing file: {file_path}")
                print("Available messages:")
                for i, grb in enumerate(grbs, 1):
                    print(f"Message {i}: {grb.shortName} {grb.typeOfLevel} {grb.level}")
                
                # Reset file pointer
                grbs.seek(0)
                message = grbs.message(target_band_number)
                
                messages.append(message)
                grbs.close()
                print(f"\033[32mExtracted band {target_band_number} from {os.path.basename(file_path)}\033[0m")
                
            except Exception as e:
                print(f"\033[31mError processing {file_path}: {str(e)}\033[0m")
                continue
        
        if not messages:
            raise ValueError("No valid messages were extracted from any files")
        
        # Verify same structure
        first_msg = messages[0]
        reference_keys = set(first_msg.keys())
        
        for i, msg in enumerate(messages[1:], 2):
            current_keys = set(msg.keys())
            if current_keys != reference_keys:
                missing_keys = reference_keys - current_keys
                extra_keys = current_keys - reference_keys
                print(f"\033[33mWarning: Message {i} has different keys:")
                if missing_keys:
                    print(f"Missing keys: {missing_keys}")
                if extra_keys:
                    print(f"Extra keys: {extra_keys}\033[0m")
        
        # Write messages using a binary approach
        with open(output_file_path, 'wb') as out:
            for msg in messages:
                valid_keys = []
                for key in msg.keys():
                    try:
                        value = msg[key]
                        if value:
                            valid_keys.append(key)
                    except Exception as e:
                        print(f"Skipping key {key} due to missing/invalid value: {e}")

                # See https://github.com/jswhit/pygrib/issues/177#issuecomment-768520311
                pygrib.tolerate_badgrib_on()

                try:
                    msg.tofile(out)
                except Exception as e:
                    print(f"\033[31mError writing message: {str(e)}")
                    print(f"Message details: {msg.shortName} {msg.typeOfLevel} {msg.level}\033[0m")
                    print(traceback.format_exc())
                    raise
        
        # Verify output file
        verify_grb = pygrib.open(output_file_path)
        msg_count = len([msg for msg in verify_grb])
        verify_grb.close()
        print(f"\033[32mVerification: Output file contains {msg_count} messages\033[0m")
        
    except Exception as e:
        print(f"\033[31mError occurred while merging files: {str(e)}\033[0m")
        raise


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

        output_path = output_directory + AWS_PREFIX + ".t" + f"{current:04d}" + "z." + AWS_POSTFIX
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

    # Verification
    num_files = len([entry for entry in os.listdir(DOWNLOAD_LOCATION_PATH) if os.path.isfile(os.path.join(DOWNLOAD_LOCATION_PATH, entry))])
    assert num_keys == num_files, "Invalid number of downloaded files: expected %i, got %i" % (num_keys, num_files)

    # Extract and consolidate all bands into new GRIB
    merged_file_name = OUTPUT_DIR_PATH + DATE + "_" + str(BAND_NUMBER) + "_MERGED.grb2"
    merge_bands(
        directory_path=DOWNLOAD_LOCATION_PATH,
        output_file_path=merged_file_name,
        target_band_number=BAND_NUMBER,
    )

    # Convert merged grib into tiff
    grib2_to_tiff(input_file_path=merged_file_name, output_file_path=OUTPUT_FILE_PATH)

    return 0

if __name__ == "__main__":
    main()