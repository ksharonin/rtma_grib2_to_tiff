#!/usr/bin/env python3.13

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
DOWNLOAD_LOCATION_PATH = "/Users/katrinasharonin/Downloads/firelab_work/grib2_to_tiff/input/"
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
    
    input_files = [entry for entry in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, entry))]

    try:
        messages = []
        for file_path in input_files:
            if not os.path.exists(file_path):
                print("Warning: file %s not found, skipping..." % file_path)
                continue
                
            grbs = pygrib.open(file_path)
            message = grbs.message(target_band_number)
            messages.append(message)
            grbs.close()
            print("Extracted band %i from %s" % (target_band_number, file_path))
        
        with open(output_file_path, 'wb') as out:
            for msg in messages:
                msg.tofile(out)
                
        print("Successfully merged %i bands into %s" % (len(messages), output_file_path))
        
        verify_grb = pygrib.open(output_file_path)
        msg_count = len([msg for msg in verify_grb])
        verify_grb.close()
        print("Verification: output file contains %i messages" % (msg_count))
        
    except Exception as e:
        print("Error occurred while merging files: %s" % (str(e)))
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
        total_minutes = hours * 60 + minutes + time_step

        new_hours = total_minutes // 60
        new_minutes = total_minutes % 60
        current = new_hours * 100 + new_minutes

        assert new_hours < 24, "Illegal new_hours value, cannot be 24+ and got %i" % new_hours
        assert new_minutes < 60, "Illegal new_minutes value, cannot be 60+ and got %i" % new_minutes

        output_path = output_directory + AWS_PREFIX + ".t" + f"{current:04d}" + "z." + AWS_POSTFIX
        key = master_key + "/" + AWS_PREFIX + ".t" + f"{current:04d}" + "z." + AWS_POSTFIX

        print("Attempting to download file key %s from bucket %s" % (key, AWS_BUCKET_NAME))

        # Download file
        download_from_s3(
            bucket_name=AWS_BUCKET_NAME,
            s3_object=key,
            output_file_path=output_path,
        )

        print("Successfully downloaded to output path: %s" % output_path)

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

    # Download all relevant GRIB files
    master_key = AWS_PREFIX + "." + DATE
    num_keys = (60*24 // TIME_STEP_MINUTES)
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
    merged_file_name = DOWNLOAD_LOCATION_PATH + DATE + "_" + str(BAND_NUMBER) + "_MERGED.grb2"
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