# -*- coding: utf-8 -*-
"""
Name:    03_CreateGriddedMetrics.py
Purpose: Runs FUSION to create gridded metrics; postprocesses products
Author:  PA Fekety, Colorado State University, patrick.fekety@colostate.edu
Date:    2021.01.14

"""

"""
Notes:
  
"""

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Import packages
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
import os
import shutil
import subprocess
import time
import rasterio as rio

# from rasterio.plot import show
import numpy as np

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Assign Variables
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
start = time.time()

# Assign a project
project = "CO_Eastern_South_Priority2_2018"
print(project)

# main output directory
dirBase = r"E:\LidarProcessing"

# directory where the final products should be saved
dirFinalProducts = r"E:\FusionRuns"
if not os.path.exists(dirFinalProducts):
    os.mkdir(dirFinalProducts)


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Define Functions
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
def cleanGrids(inFiles, outDir, fpElev):
    # Read in Elevation Raster
    with rio.open(fpElev) as src:
        rasElev = src.read()
        kwds = src.profile

    for inFile in inFiles:
        # Read in raster to be processed
        with rio.open(inFile) as src:
            rasMetric = src.read()
        # name of the metric
        metric = os.path.basename(inFile)

        rasCleanMetric = np.empty(
            rasElev.shape, dtype=rio.float32
        )  # Create empty matrix
        check = np.logical_and(
            rasMetric == -9999, rasElev != -9999
        )  # Create check raster with True/False values
        rasCleanMetric = np.where(check, 0, rasMetric)

        # Save
        if not os.path.exists(outDir):
            os.mkdir(outDir)

        fpCleanMetric = os.path.join(outDir, metric)
        with rio.open(fpCleanMetric, "w", **kwds) as dst:
            dst.write(rasCleanMetric)


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Run FUSION
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# directory for the specific lidar project; HOME_FOLDER in FUSION scripts
dirHomeFolder = os.path.join(dirBase, project)

# Directory of FUSION products
dirFusionProducts = os.path.join(dirHomeFolder, "Products")

dirFusionProcessing = os.path.join(dirHomeFolder, "Processing")
dirFusionProcessingAP = os.path.join(dirFusionProcessing, "AP")
cmdFusionMainBat = os.path.join(dirFusionProcessingAP, "APFusion.bat")
# Put in a check to see if the script has run

if not os.path.exists(os.path.join(dirFusionProducts, "complete.txt")):
    print("Running FUSION for " + project)
    subprocess.run(cmdFusionMainBat, shell=True)

    while not os.path.exists(os.path.join(dirFusionProducts, "complete.txt")):
        time.sleep(60)

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Copy Products
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

# Project-specific directory for saved outputs
dirOutProject = os.path.join(dirFinalProducts, project)
if not os.path.exists(dirOutProject):
    os.mkdir(dirOutProject)


# Directory of FUSION Metrics
dirFusionMetrics = os.path.join(dirFusionProducts, "Metrics_30METERS")
fpElev = os.path.join(dirFusionMetrics, "TOPO_elevation_30METERS.asc")


dirOutMetrics = os.path.join(dirOutProject, "FusionOutputs")
if not os.path.exists(dirOutMetrics):
    os.mkdir(dirOutMetrics)


# -----------------------------------------------------------------------------
# Topographic Metrics
# -----------------------------------------------------------------------------

rasters = []
for i in os.listdir(dirFusionMetrics):
    if i.endswith(".asc") and i.startswith("TOPO_"):
        rasters.append(i)
del i

# file paths to rasters
fpRasters = [os.path.join(dirFusionMetrics, e) for e in rasters]
outDir = os.path.join(dirOutMetrics, "TopoMetrics")
cleanGrids(
    inFiles=fpRasters, outDir=os.path.join(dirOutMetrics, "TopoMetrics"), fpElev=fpElev
)


# -----------------------------------------------------------------------------
# Height Metrics
# -----------------------------------------------------------------------------

rasters = []
for i in os.listdir(dirFusionMetrics):
    if i.endswith(".asc") and not i.startswith("TOPO_"):
        rasters.append(i)
del i

# file paths to rasters
fpRasters = [os.path.join(dirFusionMetrics, e) for e in rasters]
cleanGrids(
    inFiles=fpRasters,
    outDir=os.path.join(dirOutMetrics, "HeightMetrics"),
    fpElev=fpElev,
)


# -----------------------------------------------------------------------------
# Canopy Metrics
# -----------------------------------------------------------------------------

dirFusionCanopyMetrics = os.path.join(dirFusionProducts, "CanopyMetrics_30METERS")

rasters = []
for i in os.listdir(dirFusionCanopyMetrics):
    if i.endswith(".asc"):
        rasters.append(i)
del i

# file paths to rasters
fpRasters = [os.path.join(dirFusionCanopyMetrics, e) for e in rasters]
cleanGrids(
    inFiles=fpRasters,
    outDir=os.path.join(dirOutMetrics, "CanopyMetrics"),
    fpElev=fpElev,
)


# -----------------------------------------------------------------------------
# Intensity Metrics
# -----------------------------------------------------------------------------

fileList = os.listdir(os.path.join(dirOutMetrics, "HeightMetrics"))

intFiles = [f for f in fileList if "_int_" in f]

if len(intFiles) > 0:
    if not os.path.exists(os.path.join(dirOutMetrics, "IntensityMetrics")):
        os.mkdir(os.path.join(dirOutMetrics, "IntensityMetrics"))
    for f in intFiles:
        shutil.move(
            src=os.path.join(dirOutMetrics, "HeightMetrics", f),
            dst=os.path.join(dirOutMetrics, "IntensityMetrics", f),
        )


# -----------------------------------------------------------------------------
# Canopy Height Model
# -----------------------------------------------------------------------------

dirCHM = os.path.join(dirFusionProducts, "CanopyHeight_1p0METERS")

# raster layers
rasters = []
for i in os.listdir(dirCHM):
    if i.endswith(".asc") or i.endswith(".prj"):
        rasters.append(i)
del i

dirDestination = os.path.join(dirOutMetrics, "CHM")
if not os.path.exists(dirDestination):
    os.mkdir(dirDestination)
for raster in rasters:
    shutil.copy(
        src=os.path.join(dirCHM, raster), dst=os.path.join(dirDestination, raster)
    )
del dirDestination

# -----------------------------------------------------------------------------
# FUSION QAQC
# -----------------------------------------------------------------------------

dirQAQC = os.path.join(dirFusionProducts, "QAQC")
dirDestination = os.path.join(dirOutProject, "FusionOutputs", "QAQC")
shutil.copytree(src=dirQAQC, dst=dirDestination)
del dirDestination


# -----------------------------------------------------------------------------
# FUSION logfiles
# -----------------------------------------------------------------------------

dirLogs = os.path.join(dirFusionProducts, "Logs")
dirOutFusionParam = os.path.join(dirOutProject, "FusionParameters")
if not os.path.exists(dirOutFusionParam):
    os.mkdir(dirOutFusionParam)
dirDestination = os.path.join(dirOutFusionParam, "Logs")
shutil.copytree(src=dirLogs, dst=dirDestination)
del dirDestination


# -----------------------------------------------------------------------------
# PDAL Error Log
# -----------------------------------------------------------------------------

fpPdalErrorLog = os.path.join(dirHomeFolder, "Products", "_Error.log")
if os.path.exists(fpPdalErrorLog):
    dirPdalLog = os.path.join(dirOutProject, "PdalLog")
    if not os.path.exists(dirPdalLog):
        os.mkdir(dirPdalLog)
    shutil.copy(src=fpPdalErrorLog, dst=os.path.join(dirPdalLog, "_Error.log"))


# -----------------------------------------------------------------------------
# FUSION PRP
# -----------------------------------------------------------------------------

# copy FUSION PRP
dirPRP = os.path.join(dirHomeFolder, "PRP")
dirDestination = os.path.join(dirOutFusionParam, "PRP")
shutil.copytree(src=dirPRP, dst=dirDestination)
del dirDestination


# -----------------------------------------------------------------------------
# FUSION Setup scripts
# -----------------------------------------------------------------------------
dirOutScripts = os.path.join(dirOutFusionParam, "Scripts")
dirFusionScripts = os.path.join(dirHomeFolder, "Products", "Scripts")
dirDestination = os.path.join(dirOutScripts, "FusionSetup")
shutil.copytree(src=dirFusionScripts, dst=dirDestination)
del dirDestination


# -----------------------------------------------------------------------------
# FUSION Processing scripts
# -----------------------------------------------------------------------------

dirFusionProcessingHome = os.path.join(dirHomeFolder, "Processing")
dirDestination = os.path.join(dirOutScripts, "Processing")
shutil.copytree(src=dirFusionProcessingHome, dst=dirDestination)
del dirDestination


stop = time.time()
print(str((stop - start) / 60) + "  minutes")
