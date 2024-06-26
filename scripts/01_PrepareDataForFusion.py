# -*- coding: utf-8 -*-
"""
Name:    01_PrepareDataForFusion.py
Purpose: Copy lidar files; project to EPSG:5070; create DTMs; run QAQC
Author:  PA Fekety, Colorado State University, patrick.fekety@colostate.edu
Date:    2020.12.10

"""

"""
Notes:
  There are some sleep calls. These were added during the workflow 
    to aid in debugging. I do not believe they are still needed.
  
  Run with LAZs and No FUSION Indexing
"""

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Import packages
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
import os
import shutil
import sys
import json
import time
import subprocess
import pdal
from types import SimpleNamespace
from joblib import Parallel, delayed


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Assign Variables
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
start = time.time()

# Load config data into namespace
config_path = sys.argv[1]
with open(config_path, 'r') as f:
    config_dict = json.load(f)
config = SimpleNamespace(**config_dict)

print(config.project)

# Directory of lidar data needing to be processed (e.g., external HHD)
dirLidarOriginal = os.path.join(config.dirInputBase, config.project, "Points", "LAZ")

# main output directory
if not os.path.exists(config.dirBase):
    os.mkdir(config.dirBase)

# directory for the specific lidar project; HOME_FOLDER in FUSION scripts
dirHomeFolder = os.path.join(config.dirBase, config.project)
if not os.path.exists(dirHomeFolder):
    os.mkdir(dirHomeFolder)

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Define Functions
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

# "parallel" functions are used with joblib
def parallelProjectFunc(lidarFile, dirLidarCopy, dirLAZ5070, srsIn):
    # Function used to project the laz files to EPSG 5070

    # File name of projected LAZ file
    lasfile5070 = os.path.join(dirLAZ5070, lidarFile[:-4] + ".laz")

    # pipepline depending if the CRS is defined
    if srsIn == None:
        # The SRS is in the lidar file
        reprojectPipeline = [
            {"filename": os.path.join(dirLidarCopy, lidarFile), "type": "readers.las",},
            {"type": "filters.reprojection", "out_srs": "EPSG:5070+5703",},
            {
                "type": "writers.las",
                "scale_x": "0.01",
                "scale_y": "0.01",
                "scale_z": "0.01",
                "offset_x": "auto",
                "offset_y": "auto",
                "offset_z": "auto",
                "compression": "laszip",
                "filename": lasfile5070,
            },
        ]
    else:
        # Explicitly define SRS
        reprojectPipeline = [
            {
                "filename": os.path.join(dirLidarCopy, lidarFile),
                "type": "readers.las",
                "spatialreference": "EPSG:" + str(srsIn),
            },
            {
                "type": "filters.reprojection",
                "in_srs": "EPSG:" + str(srsIn),
                "out_srs": "EPSG:5070+5703",
            },
            {
                "type": "writers.las",
                "scale_x": "0.01",
                "scale_y": "0.01",
                "scale_z": "0.01",
                "offset_x": "auto",
                "offset_y": "auto",
                "offset_z": "auto",
                "compression": "laszip",
                "filename": lasfile5070,
            },
        ]
    pipeline = pdal.Pipeline(json.dumps(reprojectPipeline))
    try:
        pipeline.execute()
    except Exception as err:
        # Write and error file
        fpErrorLog = os.path.join(dirLAZ5070, "_Error.log")
        cmdError1 = "echo " + "PDAL Reprojection Error " + " >> " + fpErrorLog
        cmdError2 = "echo " + "Check " + lidarFile + " >> " + fpErrorLog
        cmdError3 = "echo " + str(err) + " >> " + fpErrorLog
        subprocess.run(cmdError1, shell=True)
        subprocess.run(cmdError2, shell=True)
        subprocess.run(cmdError3, shell=True)

    time.sleep(0.01)


def calcNCores(x, nCoresMax):
    # Calculates the number of cores that should be dedicated to parallel processes
    # x (list) object whose length will be compared
    # nCoresMax (int) - maximum number of processing cores available
    if nCoresMax > len(x):
        nCores = len(x)
    else:
        nCores = nCoresMax
    if nCores < 1:
        nCores = 1
    return int(nCores)


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Processing
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

# Lidar files to be processed
lidarFilesOriginal = os.listdir(dirLidarOriginal)
lidarFilesOriginal.sort()

# ----------------------------------------------------------------------------
# Process Point Data
# ----------------------------------------------------------------------------

# Copy Lidar Files
print("\tCopying Lidar Files")
dirPoints = os.path.join(dirHomeFolder, "Points")
if not os.path.exists(dirPoints):
    os.mkdir(dirPoints)

dirLidarCopy = os.path.join(dirPoints, "LidarCopy")
if not os.path.exists(dirLidarCopy):
    os.mkdir(dirLidarCopy)

for lidarFile in lidarFilesOriginal:
    shutil.copy(
        src=os.path.join(dirLidarOriginal, lidarFile),
        dst=os.path.join(dirLidarCopy, lidarFile),
    )
del lidarFile
del lidarFilesOriginal


# project to EPSG 5070
print("\tProjecting Lidar Files")

dirLAZ5070 = os.path.join(dirPoints, "LAZ5070")
if not os.path.exists(dirLAZ5070):
    os.mkdir(dirLAZ5070)

lidarFilesCopy = os.listdir(dirLidarCopy)
lidarFilesCopy.sort()

nCores = calcNCores(lidarFilesCopy, config.nCoresMax)
Parallel(n_jobs=nCores)(
    delayed(parallelProjectFunc)(lidarFile, dirLidarCopy, dirLAZ5070, config.srsIn)
    for lidarFile in lidarFilesCopy
)
del nCores
del lidarFilesCopy

# remove the copy of Lidar files
shutil.rmtree(dirLidarCopy)

# Move the Error log
if os.path.exists(os.path.join(dirLAZ5070, "_Error.log")):
    # Move the error log
    shutil.move(
        os.path.join(dirLAZ5070, "_Error.log"),
        os.path.join(dirHomeFolder, "_Error.log"),
    )


# ----------------------------------------------------------------------------
# Run QAQC
# ----------------------------------------------------------------------------
print("\tRunning FUSION Catalog\n")
dirProductHome = os.path.join(dirHomeFolder, "Products")  # FUSION PRODUCTHOME
if not os.path.exists(dirProductHome):
    os.mkdir(dirProductHome)
dirQAQC = os.path.join(dirProductHome, "QAQC")
if not os.path.exists(dirQAQC):
    os.mkdir(dirQAQC)

# Text file of lidar file paths
lidarFiles5070 = os.listdir(dirLAZ5070)
lidarFiles5070.sort()
fpLidarFilePaths = os.path.join(dirQAQC, "lidarFiles.txt")
with open(fpLidarFilePaths, "w") as f:
    for lidarFile in lidarFiles5070:
        f.write(os.path.join(dirLAZ5070, lidarFile))
        f.write("\n")
del lidarFile
del lidarFiles5070

# Run Catalog
exeCatalog = os.path.join(config.dirFUSION, "Catalog.exe")

fpQAQCOut = os.path.join(dirQAQC, "QAQC.csv")
cmdCatalog = (
    exeCatalog
    + " /rawcounts /coverage /intensity:400,0,255 /firstdensity:400,1,8 /density:400,2,16 "
    + fpLidarFilePaths
    + " "
    + fpQAQCOut
)
cmdCatalog
subprocess.run(cmdCatalog, shell=True)


# ----------------------------------------------------------------------------
# Create a few directories
# ----------------------------------------------------------------------------

dirFusionProcessing = os.path.join(dirHomeFolder, "Processing")
dirFusionProcessingAP = os.path.join(dirFusionProcessing, "AP")
if not os.path.exists(dirFusionProcessing):
    os.mkdir(dirFusionProcessing)
if not os.path.exists(dirFusionProcessingAP):
    os.mkdir(dirFusionProcessingAP)


# ----------------------------------------------------------------------------
# Error Checking
# ----------------------------------------------------------------------------
# parallelProjectFunc() creates a log file if it failed reprojecting a file
# Notify the user of the error and copy the error log

if os.path.exists(os.path.join(dirHomeFolder, "_Error.log")):
    print("\n")
    print("Errors Exist")

    # Print contents to console
    with open(os.path.join(dirHomeFolder, "_Error.log")) as f:
        print(f.read())


stop = time.time()
print(str(round(stop - start) / 60) + " minutes to complete.")
