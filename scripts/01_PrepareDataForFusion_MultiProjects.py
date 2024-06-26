# -*- coding: utf-8 -*-
"""
Name:    01_PrepareDataForFusion_MultiProjects.py
Purpose: Copy lidar files; project to EPSG:5070; create DTMs; run QAQC
Author:  PA Fekety, Colorado State University, patrick.fekety@colostate.edu
Date:    2020.12.10

"""

"""
Notes:
  This version is will run multiple lidar projects 
"""

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Import packages
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
import os
import shutil
import pdal
import json
import time
import sys
import subprocess
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


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Assign Spatial Reference Systems
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

# PDAL can read SRS directly from the LAS file. Some of the older LAS files
# do not have embeded SRS info. If you want to use the SRS in the lidar file,
# then do not include the key:pair (studyArea:epgsCode) in the dictionary.
# If the SRS is missing from the lidar file, or you want to be explicity,
# include the SRS in the dictionary

# Spatial Reference Systems of different project areas
dictSRS = dict(zip(config.project, config.srsIn))

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


def parallelRunQAQC(project, dirBase, dirFUSION):

    dirHomeFolder = os.path.join(dirBase, project)
    dirProductHome = os.path.join(dirHomeFolder, "Products")  # FUSION PRODUCTHOME
    if not os.path.exists(dirProductHome):
        os.mkdir(dirProductHome)
    dirQAQC = os.path.join(dirProductHome, "QAQC")
    if not os.path.exists(dirQAQC):
        os.mkdir(dirQAQC)

    # Text file of lidar file paths
    dirPoints = os.path.join(dirHomeFolder, "Points")
    dirLidar = os.path.join(dirPoints, "LAZ5070")
    lidarFiles5070 = os.listdir(dirLidar)
    fpLidarFilePaths = os.path.join(dirQAQC, "lidarFiles.txt")
    with open(fpLidarFilePaths, "w") as f:
        for lidarFile in lidarFiles5070:
            f.write(os.path.join(dirLidar, lidarFile))
            f.write("\n")
    del lidarFile

    # Run Catalog
    exeCatalog = os.path.join(dirFUSION, "Catalog.exe")

    fpQAQCOut = os.path.join(dirQAQC, "QAQC.csv")
    cmdCatalog = (
        exeCatalog
        + " /rawcounts /coverage /intensity:400,0,255 /firstdensity:400,2,8 /density:400,4,16 "
        + fpLidarFilePaths
        + " "
        + fpQAQCOut
    )
    cmdCatalog
    subprocess.run(cmdCatalog, shell=True)


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

for project in config.project:
    # -------------------------------------------------------------------------
    # Setup
    # -------------------------------------------------------------------------
    print(project)

    # Directory of lidar data needing to be processed (e.g., external HHD)
    dirLidarOriginal = os.path.join(config.dirInputBase, project, "Points", "LAZ")
  
    if not os.path.exists(config.dirBase):
        os.mkdir(config.dirBase)

    # directory for the specific lidar project; HOME_FOLDER in FUSION scripts
    dirHomeFolder = os.path.join(config.dirBase, project)
    if not os.path.exists(dirHomeFolder):
        os.mkdir(dirHomeFolder)

    # Lidar files to be processed
    lidarFilesOriginal = os.listdir(dirLidarOriginal)
    lidarFilesOriginal.sort()

    # -------------------------------------------------------------------------
    # Process Point Data
    # -------------------------------------------------------------------------

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
    if config.srsIn:
        srsIn = dictSRS[project]
    else:
        srsIn = None

    dirLAZ5070 = os.path.join(dirPoints, "LAZ5070")
    if not os.path.exists(dirLAZ5070):
        os.mkdir(dirLAZ5070)

    lidarFilesCopy = os.listdir(dirLidarCopy)
    lidarFilesCopy.sort()

    nCores = calcNCores(lidarFilesCopy, config.nCoresMax)
    Parallel(n_jobs=nCores)(
        delayed(parallelProjectFunc)(lidarFile, dirLidarCopy, dirLAZ5070, srsIn)
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

    # -------------------------------------------------------------------------
    # Create a few directories
    # -------------------------------------------------------------------------

    dirFusionProcessing = os.path.join(dirHomeFolder, "Processing")
    dirFusionProcessingAP = os.path.join(dirFusionProcessing, "AP")
    if not os.path.exists(dirFusionProcessing):
        os.mkdir(dirFusionProcessing)
    if not os.path.exists(dirFusionProcessingAP):
        os.mkdir(dirFusionProcessingAP)

    # -------------------------------------------------------------------------
    # Error Checking
    # -------------------------------------------------------------------------
    # parallelProjectFunc() creates a log file if it failed reprojecting a file
    # Notify the user of the error and copy the error log

    if os.path.exists(os.path.join(dirHomeFolder, "_Error.log")):
        print("\n")
        print("Errors Exist")

        # Print contents to console
        with open(os.path.join(dirHomeFolder, "_Error.log")) as f:
            print(f.read())


# ----------------------------------------------------------------------------
# Run QAQC
# ----------------------------------------------------------------------------
print("\nRunning FUSION Catalog\n")

nCores = calcNCores(config.project, config.nCoresMax)
Parallel(n_jobs=nCores)(
    delayed(parallelRunQAQC)(project, config.dirBase, config.dirFUSION) for project in config.project
)
del nCores

stop = time.time()
print(str(round(stop - start) / 60) + " minutes to complete.")
