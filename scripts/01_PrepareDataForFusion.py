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
import psutil
import glob
import numpy as np

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Assign Variables
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
start = time.time()

# Assign a project
project = "CO_Southwest_NRCS_B1_2018"

print(project)

# Directory of lidar data needing to be processed external HHD
dirData = os.path.join(r"L:\Lidar", project, "Points", "LAZ")

# FUSION directory
dirFUSION = r"C:\Fusion"

# Maximum number of processing cores
nCoresMax = 28

# Nominal size of the DTM tiles
# don't set this too large if you have dense lidar or not a lot of RAM.
DTM_TILESIZE = 2000

# The amount of buffer to be added to each DTM tile, in each direction
#  Note that 20 meters will be removed from each edge of the DTM surface.
#  This will compensate for artifacts incase the surface was created from a TIN.
DTM_BUFFER = 80

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


def parallelGridSurfaceCreate(dirBufferDTM, lidarFile, dirLasTile, dirFUSION):
    # Creates a DTM surface using the average of elevation of points in the cell
    surfacefile = os.path.join(dirBufferDTM, lidarFile[:-4] + ".dtm")
    dtmHeader = " 1 m m 0 0 2 3 "
    dataFile = os.path.join(dirLasTile, lidarFile)
    cmdFusionTIN = (
        os.path.join(dirFUSION, "GridSurfaceCreate64.exe")
        + " "
        + surfacefile
        + dtmHeader
        + dataFile
    )
    subprocess.run(cmdFusionTIN, shell=True)


def parallelCreateTIN(dirBufferDTM, lidarFile, dirLasTile, dirFUSION):
    # Creates a DTM surface from a TIN using the ground points in the cell
    surfacefile = os.path.join(dirBufferDTM, lidarFile[:-4] + ".dtm")
    dtmHeader = " 1 m m 0 0 2 3 "
    dataFile = os.path.join(dirLasTile, lidarFile)
    cmdFusionTIN = (
        os.path.join(dirFUSION, "TINSurfaceCreate.exe")
        + " "
        + surfacefile
        + dtmHeader
        + dataFile
    )
    subprocess.run(cmdFusionTIN, shell=True)


def parallelReTile(i, extents, dirLasTile, project, dirGroundPoints):
    # Function that retiles laz files
    A = str(round(extents[i][0]))
    B = str(round(extents[i][3]))
    fpCropOut = os.path.join(dirLasTile, project + "_" + A + "_" + B + ".laz")

    cropPipeline = [
        os.path.join(dirGroundPoints, "*.laz"),
        {
            "type": "filters.crop",
            "bounds": "(" + str(extents[i][0:2]) + "," + str(extents[i][2:4]) + ")",
        },
        {"type": "writers.las", "filename": fpCropOut},
    ]
    pipeline = pdal.Pipeline(json.dumps(cropPipeline))
    pipeOut = pipeline.execute()
    # Delete LAZ if no points were written
    if pipeOut == 0:
        os.remove(fpCropOut)


def parallelExtractGndRtns(lidarFile, dirLidar, dirGroundPoints):
    # Creates an LAZ file of just returns that can be used to create ground surfaces
    filterPipeline = [
        os.path.join(dirLidar, lidarFile),
        {"type": "filters.range", "limits": "Classification[2:2], Classification[8:8]"},
        {"type": "writers.las", "filename": os.path.join(dirGroundPoints, lidarFile)},
    ]

    pipeline = pdal.Pipeline(json.dumps(filterPipeline))
    pipeline.execute()


def doesIntersects(lidarExtent, dtmExtent):
    # function to determine in a lidar tile intersects a DTM tile
    # lidarExtent (list) - extent of the lidar tile [xMin, xMax, yMin, yMax]
    # dtmExtent (list) - extent of the DTM tile [xMin, xMax, yMin, yMax]
    lidarXMin = lidarExtent[0]
    lidarXMax = lidarExtent[1]
    lidarYMin = lidarExtent[2]
    lidarYMax = lidarExtent[3]

    dtmXMin = dtmExtent[0]
    dtmXMax = dtmExtent[1]
    dtmYMin = dtmExtent[2]
    dtmYMax = dtmExtent[3]

    # these four conditions must be True for a lidar tile to intersect the DTM tile
    cond1 = lidarXMax >= dtmXMin
    cond2 = lidarXMin <= dtmXMax
    cond3 = lidarYMin <= dtmYMax
    cond4 = lidarYMax >= dtmYMin

    intersects = all([cond1, cond2, cond3, cond4])
    return intersects


def parallelRetile(i, dictDtmLidar, dirPoints, dictRetile, dirLasTile):
    # List of the lidar file paths in the DTM tile
    fpLidars = []
    for j in dictDtmLidar[i]:
        fpLidars.append(os.path.join(dirPoints, "Ground", j))

    # File Name for the DTM tile. Based on the lower left coordinates
    A = str(round(dictRetile[i][0]))
    B = str(round(dictRetile[i][2]))
    fpCropOut = os.path.join(dirLasTile, project + "_" + A + "_" + B + ".laz")

    for k in range(len(fpLidars)):
        fp = fpLidars[k]
        tempFP = os.path.join(
            dirLasTile, project + "_" + A + "_" + B + "_" + str(k) + ".laz"
        )
        crop3Pipeline = [
            fp,
            {
                "type": "filters.crop",
                "bounds": "("
                + str(dictRetile[i][0:2])
                + ","
                + str(dictRetile[i][2:4])
                + ")",
            },
            {"type": "writers.las", "filename": tempFP},
        ]
        pipeline = pdal.Pipeline(json.dumps(crop3Pipeline))
        pipeOut = pipeline.execute()
        if pipeOut == 0:
            os.remove(tempFP)

    # Merge the temp files back to a single
    mergePipeline = []
    for m in glob.glob(
        os.path.join(dirLasTile, project + "_" + A + "_" + B + "_*.laz")
    ):
        mergePipeline.append(m)
    mergePipeline.append(
        {
            "type": "filters.crop",
            "bounds": "("
            + str(dictRetile[i][0:2])
            + ","
            + str(dictRetile[i][2:4])
            + ")",
        }
    )
    mergePipeline.append({"type": "writers.las", "filename": fpCropOut})

    pipeline = pdal.Pipeline(json.dumps(mergePipeline))
    pipeOut = pipeline.execute()

    # delete temp files
    for m in glob.glob(
        os.path.join(dirLasTile, project + "_" + A + "_" + B + "_*.laz")
    ):
        os.remove(m)


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


# creating buffered tiles for DTM
print("\tExtracting Ground Returns")
dirGroundPoints = os.path.join(dirPoints, "Ground")
if not os.path.exists(dirGroundPoints):
    os.mkdir(dirGroundPoints)

lidarFiles5070 = os.listdir(dirLidar)


nCores = calcNCores(lidarFiles5070, nCoresMax)
Parallel(n_jobs=nCores)(
    delayed(parallelExtractGndRtns)(lidarFile, dirLidar, dirGroundPoints)
    for lidarFile in lidarFiles5070
)
del nCores


print("\tCalculating Extents of Buffered Lidar Tiles for DTM")
# A better way to get the lidar project extent. This also returns the individual
#  tile extents
lidarFilesGround = os.listdir(dirGroundPoints)
dictExtents = {}
for lidarFile in lidarFilesGround:
    fpLidarFile = os.path.join(dirGroundPoints, lidarFile)
    cmdMetadata = "pdal info  " + fpLidarFile + " --metadata"
    r = subprocess.run(cmdMetadata, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    json_info = json.loads(r.stdout.decode())
    minx = json_info["metadata"]["minx"]
    maxx = json_info["metadata"]["maxx"]
    miny = json_info["metadata"]["miny"]
    maxy = json_info["metadata"]["maxy"]
    dictExtents[lidarFile] = [minx, maxx, miny, maxy]
del minx
del maxx
del miny
del maxy


# Extent of Lidar Collection
xMinCollection = min([item[0] for item in list(dictExtents.values())])
xMaxCollection = max([item[1] for item in list(dictExtents.values())])
yMinCollection = min([item[2] for item in list(dictExtents.values())])
yMaxCollection = max([item[3] for item in list(dictExtents.values())])

# Create LAS tiles that are 4160 meters square (DTM_TILESIZE meters with a DTM_BUFFER meter buffer)
nDTMRow = round(abs((yMinCollection - yMaxCollection) / DTM_TILESIZE) + 0.5)
nDTMCol = round(abs((xMinCollection - xMaxCollection) / DTM_TILESIZE) + 0.5)

# Extents for retiled lasfiles
dictRetile = {}
for i in range(nDTMRow):
    for j in range(nDTMCol):
        tileXMin = xMinCollection + j * DTM_TILESIZE - DTM_BUFFER
        tileXMax = xMinCollection + j * DTM_TILESIZE + (DTM_TILESIZE + DTM_BUFFER)
        tileYMin = yMinCollection + i * DTM_TILESIZE - DTM_BUFFER
        tileYMax = yMinCollection + i * DTM_TILESIZE + (DTM_TILESIZE + DTM_BUFFER)

        rowName = str(i)
        colName = str(j)
        while len(rowName) < 5:
            rowName = "0" + rowName
        while len(colName) < 5:
            colName = "0" + colName
        retileName = "dtm_" + rowName + "_" + colName
        dictRetile[retileName] = [tileXMin, tileXMax, tileYMin, tileYMax]


dictDtmLidar = {}
for dtmKey in list(dictRetile.keys()):
    lidarTiles = []
    for lidarKey in list(dictExtents.keys()):

        if doesIntersects(dictExtents[lidarKey], dictRetile[dtmKey]):
            lidarTiles.append(lidarKey)
        if len(lidarTiles) > 0:
            dictDtmLidar[dtmKey] = lidarTiles


dirLasTile = os.path.join(dirPoints, "GroundTiles")
if not os.path.exists(dirLasTile):
    os.mkdir(dirLasTile)

# total amount of memory
memoryTotal = psutil.virtual_memory().total
memoryTotal / 2 ** 30


# Memory available
memoryAvailable = psutil.virtual_memory().available
memoryAvailable / 2 ** 30


# Get file size of the lidar files in each tile
fileSize = {}
for dtmLidarKey in list(dictDtmLidar.keys()):
    dtmLidarKey
    size = 0
    for f in dictDtmLidar[dtmLidarKey]:
        size = size + os.path.getsize(os.path.join(dirPoints, "Ground", f))
    fileSize[dtmLidarKey] = size

# The DTM tile that will need the largest amount of memory
memoryMaxNeeded = max(fileSize.values())
memoryMaxNeeded * 12 / 2 ** 30  # assume a compression factor of 12 and having 1 copy in memory in GB

# Approximate number of cores available
(memoryAvailable) / (memoryMaxNeeded * 12)


# dictionary key with largest memory needed
list(fileSize.keys())[list(fileSize.values()).index(memoryMaxNeeded)]
list(fileSize.keys())[list(fileSize.values()).index(min(fileSize.values()))]


# Try using 80% of the calculated memory
tempMax = np.trunc((memoryAvailable) / (memoryMaxNeeded * 12) * 0.8)
nCores = calcNCores(dictDtmLidar, tempMax)
Parallel(n_jobs=nCores)(
    delayed(parallelRetile)(i, dictDtmLidar, dirPoints, dictRetile, dirLasTile)
    for i in list(dictDtmLidar.keys())
)
del nCores
del tempMax


# print("\tCreating Buffered Lidar Tiles for DTM")
# nCores = calcNCores(dictDtmLidar, nCoresMax)
# Parallel(n_jobs=nCores)(
#     delayed(parallelRetile)(i, dictDtmLidar, dirPoints, dictRetile, dirLasTile)
#     for i in list(dictDtmLidar.keys())
# )
# del nCores


# ----------------------------------------------------------------------------
# Create DTMs
# ----------------------------------------------------------------------------

# Directory for temporary DTM products
print("\tCreating Temporary DTMs")
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


# Use FUSION to create DTM surface
# can either use parallelGridSurfaceCreate or parallelCreateTIN
nCores = calcNCores(lidarFilesGroundTiles, nCoresMax)
Parallel(n_jobs=nCores)(
    delayed(parallelGridSurfaceCreate)(dirBufferDTM, lidarFile, dirLasTile, dirFUSION)
    for lidarFile in lidarFilesGroundTiles
)
del nCores


# Remove some of the buffer that was introduced in the lasTile command.
# TIN triangulations can break down at the edges.
print("\tCreating Final DTMs")
dirDeliverables = os.path.join(dirHomeFolder, "Deliverables")
if not os.path.exists(dirDeliverables):
    os.mkdir(dirDeliverables)
dirClipDTM = os.path.join(dirDeliverables, "DTM")
if not os.path.exists(dirClipDTM):
    os.mkdir(dirClipDTM)
exeClipDTM = os.path.join(dirFUSION, "ClipDTM64.exe")
bufferedDTMs = os.listdir(dirBufferDTM)
dirInDTM = dirBufferDTM
dirOutDTM = dirClipDTM


for bufferedDTM in bufferedDTMs:
    # def parallelClipDTM(fpClipDTM, bufferedDTM, dirInDTM, dirOutDTM)
    # removes 20 meters from every DTM
    fpInDTM = os.path.join(dirInDTM, bufferedDTM)
    fpOutDTM = os.path.join(dirOutDTM, bufferedDTM)
    # Runs FUSION ClipDTM to create index files
    cmdClipDTM = (
        exeClipDTM + " /shrink " + fpInDTM + " " + fpOutDTM + " 20.0 20.0 20.0 20.0"
    )
    subprocess.run(cmdClipDTM, shell=True)
    time.sleep(0.1)

# remove intermediate directories to save space
shutil.rmtree(dirTempDTM)
shutil.rmtree(dirLasTile)
shutil.rmtree(dirGroundPoints)

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
