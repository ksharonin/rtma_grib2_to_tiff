# GRIB2 To TIFF
Author: Katrina Sharonin

## Summary
`main.py` runs the workflow. The script queries NOAA RTMA s3 buckets given a date, dataset settings, time step.
(24 hrs / time step) number files are downloaded. The script opens each GRIB file, slices out the input band index, and creates a new GRIB which merges all bands together. Then, the GRIB is converted to a TIFF file.

## Set up
Create a venv via terminal:
`python3.13 -m venv ./venv `
Activate with:
`source ./venv/bin/activate` 

Required packages (install via pip, conda, etc):
- boto3
- gdal
- cfgrib
- pygrib

## Settings and Running the Script
TODO