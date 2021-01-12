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

# Lidar projects that need to be processed.
projects = [
    "CO_Eastern_South_Priority2_2018",  # pass
    "CO_ElPasoCoCentral_2018",  # fail Unable to fetch data and convert as requested: ScanAngleRank:float(189.432) -> signed char
    # Looks like it is caused by a bad LAS file: LD31261359.las and LD31291374.las
    "CO_ElPasoCoSouth_2018",  # pass
    "CO_GunnisonCo_2016",  # pass
    "CO_HuerfanoCo_2018",  # pass
    "CO_Southwest_NRCS_B3_2018",  # pass
]

# Assign a project
project = projects[1]
print(project)

# external HHD
dirData = os.path.join(r"F:\Lidar", project, "Points", "LAZ")


# Lastools Binaries
dirLASTools = r"F:\LAStools\bin"

# FUSION directory
dirFUSION = r"c:\Fusion"


# Maximum number of processing cores
# AZAD: 10
# DENALI: 28
nCoresMax = 12


# main output directory
dirWD = r"E:\CMS2WorkflowTest"
if not os.path.exists(dirWD):
    os.mkdir(dirWD)

# directory for the specific lidar project; HOME_FOLDER in FUSION scripts
dirHomeFolder = os.path.join(dirWD, project)
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
    "CO_ARRA_GrandCo_2010": 3743,
    "CO_ARRA_LarimerCo_2010": 3743,
    "CO_ARRA_ParkCo_2010": 3743,
    "CO_ArapahoeCo_2018": 6428,
    "CO_ArkansasValley_2010": 3743,
    "CO_BoulderCreekCZOSnowOff_2010": 26913,
    "CO_CentralEasternPlains_1_2020": 6342,
    "CO_Central_Western_2016": 6428,
    "CO_CheesmanLake_2004": 26913,
    "CO_DenverDNC_2008": 26913,
    "CO_Denver_2008": 26913,
    "CO_Eastern_B1_2018": 6430,
    "CO_Eastern_B2_QL1_Central_2018": 6428,
    "CO_Eastern_B2_QL2_Central_2018": 6428,
    "CO_Eastern_B2_QL2_North_2018": 6430,
    "CO_Eastern_B5_2018": 6432,
    "CO_Eastern_South_Priority2_2018": 6432,
    "CO_ElPasoCoCentral_2018": 6428,
    "CO_ElPasoCoSouth_2018": 6432,
    "CO_FremontCo_2016": 6428,
    "CO_GunnisonCo_2016_LAS": 6342,
    "CO_HuerfanoCo_2018": 6432,
    "CO_Kremmling_2012": 6342,
    "CO_LaPlataCo_CHAMP_2018": 2233,
    "CO_LarimerCo_CHAMP_2015": 2231,
    "CO_LarimerCo_GlenHaven_2013": 2231,
    "CO_LovelandE_2016": 6430,
    "CO_LovelandW_2016": 6430,
    "CO_MesaCo_QL1_2016": 6342,
    "CO_MesaCo_QL2_2015": 6341,
    "CO_MesaCo_QL2_UTM13_2015": 6342,
    "CO_NESEColorado_1_2019": 6432,
    "CO_NESEColorado_2_2019": 6432,
    "CO_NESEColorado_3_2019": 6432,
    "CO_NiwotRidgeLTER_2005": 26913,
    "CO_PitkinCo_2016": 6428,
    "CO_PuebloCo_2018": 6432,
    "CO_RaleighPeak_2010": 26913,
    "CO_RoanPlateauNorth_2007": 32612,
    "CO_RoanPlateauSouth_2007": 32612,
    "CO_RouttCo_2016": 6430,
    "CO_SanIsabelNFDebrisFlow_2008": 26913,
    "CO_SanJuan_NF_2017": 6342,
    "CO_San_Luis_Valley_2011": 3720,
    "CO_SilverPlume_2005": 26913,
    "CO_SlumgullionLandslide184_2015": 6342,
    "CO_SlumgullionLandslide188_2015": 6342,
    "CO_SlumgullionLandslide191_2015": 6342,
    "CO_SoPlatteRiver_Lot1_2013": 6342,
    "CO_SoPlatteRiver_Lot2a_2013": 6342,
    "CO_SoPlatteRiver_Lot5_2013": 6342,
    "CO_SoPlatte_Lot3_2013": 6342,
    "CO_Southwest_NRCS_B1_2018": 5070,
    "CO_Southwest_NRCS_B2_2018": 5070,
    "CO_Southwest_NRCS_B3_2018": 5070,
    "CO_TellerCo_2016": 6428,
    "CO_TellerCo_CHAMP_2014": 2232,
    "CO_UCBolderFlatIrons_2010": 26913,
    "CO_WestBijouCreek_2007": 26913,
    "NM_Animas_2014": 6342,
    "NM_Northeast_B5_2018": 6342,
}

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Define Functions
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

# "parallel" functions are used with joblib


def parallelProjectFunc(lidarFile, dirData, dirLAZ5070, srsIn):
    # Used to project the laz files to EPSG 5070

    # lidarFile2 = lidarFile[:-4] + ".las"  # testing if las or laz is faster
    lasfile5070 = os.path.join(dirLAZ5070, lidarFile)

    # pipepline depending if the CRS is defined
    if srsIn == None:
        # The SRS is in the lidar file
        reprojectPipeline = [
            {"filename": os.path.join(dirData, lidarFile), "type": "readers.las",},
            {"type": "filters.reprojection", "out_srs": "EPSG:5070",},
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
                "filename": os.path.join(dirData, lidarFile),
                "type": "readers.las",
                "spatialreference": "EPSG:" + str(srsIn),
            },
            {
                "type": "filters.reprojection",
                "in_srs": "EPSG:" + str(srsIn),
                "out_srs": "EPSG:5070",
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


def parallelExtractGndRtns(lidarFile, dirLAZ5070, dirGroundPoints, dirLASTools):
    # Used to create LAZ files of ground points
    fpLidarIn = os.path.join(dirLAZ5070, lidarFile)
    fpLidarOut = os.path.join(dirGroundPoints, lidarFile)
    exeLas2Las = os.path.join(dirLASTools, "las2las.exe")
    cmdLas2Las = exeLas2Las + " -i " + fpLidarIn + " -keep_class 2 8 -o " + fpLidarOut
    subprocess.run(cmdLas2Las, shell=True)
    time.sleep(0.1)


def parallelBlast2Dem(lidarFile, dirLasTile, dirDTMBuffer, dirLASTools):
    # Creates .DTM files for ground surfaces
    exeBlast2Dem = os.path.join(dirLASTools, "blast2dem.exe")
    fpDtm = os.path.join(dirDTMBuffer, lidarFile[:-4] + ".dtm")
    fpLidarFile = os.path.join(dirLasTile, dirLasTile, lidarFile)
    cmdBlast2Dem = (
        exeBlast2Dem + " -i " + fpLidarFile + " -o " + fpDtm + " -odtm -step 1"
    )
    subprocess.run(cmdBlast2Dem)
    time.sleep(0.1)


def parallelLastoolsIndex(lidarFile, dirGroundPoints, dirLASTools):
    # Used to create LAZ files of ground points
    fpLidarIn = os.path.join(dirGroundPoints, lidarFile)
    exeLasIndex = os.path.join(dirLASTools, "lasindex64.exe")
    cmdLas2Las = exeLasIndex + " -i " + fpLidarIn
    subprocess.run(cmdLas2Las, shell=True)
    time.sleep(0.1)


def parallelClipDTM(fpClipDTM, bufferedDTM, dirInDTM, dirOutDTM):
    # removes 20 meters from every DTM
    fpInDTM = os.path.join(dirInDTM, bufferedDTM)
    fpOutDTM = os.path.join(dirOutDTM, bufferedDTM)
    # Runs FUSION ClipDTM to create index files
    cmdClipDTM = (
        fpClipDTM + " /shrink " + fpInDTM + " " + fpOutDTM + " 20.0 20.0 20.0 20.0"
    )
    subprocess.run(cmdClipDTM, shell=True)
    time.sleep(0.1)


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Processing
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

# test set of lidar files
lidarFiles = os.listdir(dirData)
lidarFiles.sort()


# ----------------------------------------------------------------------------
# Process Point Data
# ----------------------------------------------------------------------------

# Copy Lidar Files
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
srsNeedsDefining = project in dictSRS
if srsNeedsDefining:
    srsIn = dictSRS[project]
else:
    srsIn = None


dirLAZ5070 = os.path.join(dirPoints, "LAZ5070")
if not os.path.exists(dirLAZ5070):
    os.mkdir(dirLAZ5070)

if nCoresMax > len(lidarFiles):
    nCores = len(lidarFiles)
else:
    nCores = nCoresMax
Parallel(n_jobs=nCores)(
    delayed(parallelProjectFunc)(lidarFile, dirData, dirLAZ5070, srsIn)
    for lidarFile in lidarFiles
)
del nCores

# remove the copy of ╧original LAZs
shutil.rmtree(dirLAZ)


# creating buffered tiles for DTM
dirGroundPoints = os.path.join(dirPoints, "Ground")
if not os.path.exists(dirGroundPoints):
    os.mkdir(dirGroundPoints)

lidarFiles5070 = os.listdir(dirLAZ5070)
if not os.path.exists(dirLAZ5070):
    os.mkdir(dirLAZ5070)

if nCoresMax > len(lidarFiles5070):
    nCores = len(lidarFiles5070)
else:
    nCores = nCoresMax
Parallel(n_jobs=nCores)(
    delayed(parallelExtractGndRtns)(lidarFile, dirLAZ5070, dirGroundPoints, dirLASTools)
    for lidarFile in lidarFiles5070
)
del nCores


lidarFilesGround = os.listdir(dirGroundPoints)
if nCoresMax > len(lidarFilesGround):
    nCores = len(lidarFilesGround)
else:
    nCores = nCoresMax
Parallel(n_jobs=nCores)(
    delayed(parallelLastoolsIndex)(lidarFile, dirGroundPoints, dirLASTools)
    for lidarFile in lidarFilesGround
)
del nCores

# tiles of buffered ground points
exeLasTile = os.path.join(dirLASTools, "lastile.exe")
dirLasTile = os.path.join(dirPoints, "GroundTiles")
if not os.path.exists(dirLasTile):
    os.mkdir(dirLasTile)

if nCoresMax > len(lidarFilesGround):
    nCores = len(lidarFilesGround)
else:
    nCores = nCoresMax
cmdLasTile = (
    exeLasTile
    + " -i "
    + dirGroundPoints
    + "\*.laz -merged -tile_size 5000 -buffer 80 -o "
    + os.path.join(dirLasTile, project)
    + " -olaz -cores "
    + str(nCores)
)
cmdLasTile
subprocess.run(cmdLasTile, shell=True)
del nCores

# Index tiles of buffered ground points
lidarFilesBufferedGround = os.listdir(dirLasTile)
if nCoresMax > len(lidarFilesBufferedGround):
    nCores = len(lidarFilesBufferedGround)
else:
    nCores = nCoresMax
Parallel(n_jobs=nCores)(
    delayed(parallelLastoolsIndex)(lidarFile, dirLasTile, dirLASTools)
    for lidarFile in lidarFilesBufferedGround
)
del nCores

# ----------------------------------------------------------------------------
# Create DTMs
# ----------------------------------------------------------------------------

# Directory for temporary DTM products
dirTempDTM = os.path.join(dirHomeFolder, "DTM_Temp")
if not os.path.exists(dirTempDTM):
    os.mkdir(dirTempDTM)

# DTMs that have been buffered by 80 meter
dirBufferDTM = os.path.join(dirTempDTM, "DTM_Buffer")
if not os.path.exists(dirBufferDTM):
    os.mkdir(dirBufferDTM)
lidarFilesGroundTilesTemp = os.listdir(dirLasTile)
lidarFilesGroundTiles = []
for e in lidarFilesGroundTilesTemp:
    if e.endswith(".laz"):
        lidarFilesGroundTiles.append(e)
del lidarFilesGroundTilesTemp

if nCoresMax > len(lidarFilesGroundTiles):
    nCores = len(lidarFilesGroundTiles)
else:
    nCores = nCoresMax
Parallel(n_jobs=nCores)(
    delayed(parallelBlast2Dem)(lidarFile, dirLasTile, dirBufferDTM, dirLASTools)
    for lidarFile in lidarFilesGroundTiles
)
del nCores


# Remove some of the buffer that was introduced in the lasTile command.
# TIN triangulations can break down at the edges.
dirDeliverables = os.path.join(dirHomeFolder, "Deliverables")
if not os.path.exists(dirDeliverables):
    os.mkdir(dirDeliverables)
dirClipDTM = os.path.join(dirDeliverables, "DTM")
if not os.path.exists(dirClipDTM):
    os.mkdir(dirClipDTM)
exeClipDTM = os.path.join(dirFUSION, "ClipDTM.exe")
bufferedDTMs = os.listdir(dirBufferDTM)
dirInDTM = dirBufferDTM
dirOutDTM = dirClipDTM


if nCoresMax > len(bufferedDTMs):
    nCores = len(bufferedDTMs)
else:
    nCores = nCoresMax
Parallel(n_jobs=nCores)(
    delayed(parallelClipDTM)(exeClipDTM, bufferedDTM, dirInDTM, dirOutDTM)
    for bufferedDTM in bufferedDTMs
)
del nCores

# remove intermediate directories to save space
shutil.rmtree(dirTempDTM)
shutil.rmtree(dirLasTile)
shutil.rmtree(dirGroundPoints)

# ----------------------------------------------------------------------------
# Run QAQC
# ----------------------------------------------------------------------------

dirProductHome = os.path.join(dirHomeFolder, "Products")  # FUSION PRODUCTHOME
if not os.path.exists(dirProductHome):
    os.mkdir(dirProductHome)
dirQAQC = os.path.join(dirProductHome, "QAQC")
if not os.path.exists(dirQAQC):
    os.mkdir(dirQAQC)


# Text file of lidar file paths
fpLidarFilePaths = os.path.join(dirQAQC, "lidarFiles.txt")
with open(fpLidarFilePaths, "w") as f:
    for lidarFile in lidarFiles5070:
        f.write(os.path.join(dirLAZ5070, lidarFile))
        f.write("\n")
del lidarFile


# =============================================================================
# #Create FUSION Index Files
# exeCatalog = os.path.join(dirFUSION, "Catalog.exe")
# lidarFilePaths = [os.path.join(dirLAZ5070, e) for e in lidarFiles]
# fpCatOut = os.path.join(dirQAQC, "Cat.csv")
#
# lidarFilePaths = [os.path.join(dirLAZ5070, e) for e in lidarFiles]
#
# def parallelCatalogIndexFile(exeCatalog, lidarFilePath):
#     # Runs FUSION catalog to create index files
#     cmdCatalog = exeCatalog + " /index " + lidarFilePath
#     subprocess.run(cmdCatalog, shell=True)
#     time.sleep(0.1)
#
#
# start = time.time()
# if nCoresMax > len(bufferedDTMs):
#     nCores = len(bufferedDTMs)
# else:
#     nCores = nCoresMax
# Parallel(n_jobs=nCores)(
#     delayed(parallelCatalogIndexFile)(exeCatalog, lidarFilePath)
#     for lidarFilePath in lidarFilePaths
# )
# =============================================================================

# Run Catalog

exeCatalog = os.path.join(dirFUSION, "Catalog.exe")
lidarFilePaths = [os.path.join(dirLAZ5070, e) for e in lidarFiles]

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


stop = time.time()
stop - start


# ----------------------------------------------------------------------------
# Error Checking
# ----------------------------------------------------------------------------
# parallelProjectFunc() creates a log file if it failed reprojecting a file
# Notify the user of the error and copy the error log


if os.path.exists(os.path.join(dirLAZ5070, "_Error.log")):
    print("\n")
    print("Errors Exist")

    # copy the error log
    shutil.copy(
        os.path.join(dirLAZ5070, "_Error.log"),
        os.path.join(dirHomeFolder, "_Error.log"),
    )

    # Print contents to console
    with open(os.path.join(dirLAZ5070, "_Error.log")) as f:
        print(f.read())