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
    construction to view suspect outputs from the lastools. I do not believe
    they are still needed.
  
  Run with LAZs and No FUSION Indexing
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
from joblib import Parallel, delayed
import subprocess


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Assign Variables
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
start = time.time()

# Assign a project
project = "CO_ARRA_ParkCo_2010"

print(project)

# Directory of lidar data needing to be processed external HHD
dirData = os.path.join(r"L:\Lidar", project, "Points", "LAZ")

# FUSION directory
dirFUSION = r"C:\Fusion"

# Maximum number of processing cores
nCoresMax = 26

# main output directory
dirBase = r"D:\LidarProcessing"
if not os.path.exists(dirBase):
    os.mkdir(dirBase)

# directory for the specific lidar project; HOME_FOLDER in FUSION scripts
dirHomeFolder = os.path.join(dirBase, project)
if not os.path.exists(dirHomeFolder):
    os.mkdir(dirHomeFolder)

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
dictSRS = {
    # "CO_ARRA_GrandCo_2010": 3743,
    # "CO_ARRA_LarimerCo_2010": 3743,
    # "CO_ARRA_ParkCo_2010": 3743,
    # "CO_ArapahoeCo_2018": 6428,
    # "CO_ArkansasValley_2010": 3743,
    # "CO_BoulderCreekCZOSnowOff_2010": 26913,
    # "CO_CentralEasternPlains_1_2020": 6342,
    # "CO_Central_Western_2016": 6428,
    # "CO_CheesmanLake_2004": 26913,
    # "CO_DenverDNC_2008": 26913,
    # "CO_Denver_2008": 26913,
    # "CO_Eastern_B1_2018": 6430,
    # "CO_Eastern_B2_QL1_Central_2018": 6428,
    # "CO_Eastern_B2_QL2_Central_2018": 6428,
    # "CO_Eastern_B2_QL2_North_2018": 6430,
    # "CO_Eastern_B5_2018": 6432,
    # "CO_Eastern_South_Priority2_2018": 6432,
    # "CO_ElPasoCoCentral_2018": 6428,
    # "CO_ElPasoCoSouth_2018": 6432,
    # "CO_FremontCo_2016": 6428,
    # "CO_GunnisonCo_2016_LAS": 6342,
    # "CO_HuerfanoCo_2018": 6432,
    # "CO_Kremmling_2012": 6342,
    # "CO_LaPlataCo_CHAMP_2018": 2233,
    # "CO_LarimerCo_CHAMP_2015": 2231,
    # "CO_LarimerCo_GlenHaven_2013": 2231,
    # "CO_LovelandE_2016": 6430,
    # "CO_LovelandW_2016": 6430,
    # "CO_MesaCo_QL1_2016": 6342,
    # "CO_MesaCo_QL2_2015": 6341,
    # "CO_MesaCo_QL2_UTM13_2015": 6342,
    # "CO_NESEColorado_1_2019": 6432,
    # "CO_NESEColorado_2_2019": 6432,
    # "CO_NESEColorado_3_2019": 6432,
    # "CO_NiwotRidgeLTER_2005": 26913,
    # "CO_PitkinCo_2016": 6428,
    # "CO_PuebloCo_2018": 6432,
    # "CO_RaleighPeak_2010": 26913,
    # "CO_RoanPlateauNorth_2007": 32612,
    # "CO_RoanPlateauSouth_2007": 32612,
    # "CO_RouttCo_2016": 6430,
    # "CO_SanIsabelNFDebrisFlow_2008": 26913,
    # "CO_SanJuan_NF_2017": 6342,
    # "CO_San_Luis_Valley_2011": 3720,
    # "CO_SilverPlume_2005": 26913,
    # "CO_SlumgullionLandslide184_2015": 6342,
    # "CO_SlumgullionLandslide188_2015": 6342,
    # "CO_SlumgullionLandslide191_2015": 6342,
    # "CO_SoPlatteRiver_Lot1_2013": 6342,
    # "CO_SoPlatteRiver_Lot2a_2013": 6342,
    # "CO_SoPlatteRiver_Lot5_2013": 6342,
    # "CO_SoPlatte_Lot3_2013": 6342,
    # "CO_Southwest_NRCS_B1_2018": 5070,
    # "CO_Southwest_NRCS_B2_2018": 5070,
    # "CO_Southwest_NRCS_B3_2018": 5070,
    # "CO_TellerCo_2016": 6428,
    # "CO_TellerCo_CHAMP_2014": 2232,
    # "CO_UCBolderFlatIrons_2010": 26913,
    # "CO_WestBijouCreek_2007": 26913,
    # "NM_Animas_2014": 6342,
    # "NM_Northeast_B5_2018": 6342,
}

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Define Functions
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

# "parallel" functions are used with joblib
def parallelProjectFunc(lidarFile, dirLAZ, dirLidar, srsIn):
    # Used to project the laz files to EPSG 5070
    lasfile5070 = os.path.join(dirLidar, lidarFile)

    # pipepline depending if the CRS is defined
    if srsIn == None:
        # The SRS is in the lidar file
        reprojectPipeline = [
            {"filename": os.path.join(dirLAZ, lidarFile), "type": "readers.las",},
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
                "filename": os.path.join(dirLAZ, lidarFile),
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
        fpErrorLog = os.path.join(dirLidar, "_Error.log")
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
lidarFiles = os.listdir(dirData)
lidarFiles.sort()


# ----------------------------------------------------------------------------
# Process Point Data
# ----------------------------------------------------------------------------

# Copy Lidar Files
print("\tCopying Lidar Files")
dirPoints = os.path.join(dirHomeFolder, "Points")
if not os.path.exists(dirPoints):
    os.mkdir(dirPoints)

dirLAZ = os.path.join(dirPoints, "LAZ")
if not os.path.exists(dirLAZ):
    os.mkdir(dirLAZ)

for lidarFile in lidarFiles:
    shutil.copy(
        src=os.path.join(dirData, lidarFile), dst=os.path.join(dirLAZ, lidarFile)
    )
del lidarFile


# project to EPSG 5070
print("\tProjecting Lidar Files")
srsNeedsDefining = project in dictSRS
if srsNeedsDefining:
    srsIn = dictSRS[project]
else:
    srsIn = None


dirLidar = os.path.join(dirPoints, "LAZ5070")
if not os.path.exists(dirLidar):
    os.mkdir(dirLidar)

nCores = calcNCores(lidarFiles, nCoresMax)
Parallel(n_jobs=nCores)(
    delayed(parallelProjectFunc)(lidarFile, dirLAZ, dirLidar, srsIn)
    for lidarFile in lidarFiles
)
del nCores

# remove the copy of original LAZs
shutil.rmtree(dirLAZ)

# Move the Error log
if os.path.exists(os.path.join(dirLidar, "_Error.log")):
    # Move the error log
    shutil.move(
        os.path.join(dirLidar, "_Error.log"), os.path.join(dirHomeFolder, "_Error.log"),
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
lidarFiles5070 = os.listdir(dirLidar)
fpLidarFilePaths = os.path.join(dirQAQC, "lidarFiles.txt")
with open(fpLidarFilePaths, "w") as f:
    for lidarFile in lidarFiles5070:
        f.write(os.path.join(dirLidar, lidarFile))
        f.write("\n")
del lidarFile


# Run Catalog
exeCatalog = os.path.join(dirFUSION, "Catalog.exe")
lidarFilePaths = [os.path.join(dirLidar, e) for e in lidarFiles]

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
