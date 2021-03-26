# CMS2LidarProcessing  
CMS Phase 2 Lidar Processing Workflow  

**Important note**: It is best to clone this repository instead of using the download button. Some users have noticed problems with end of line characters when downloading the repo on Windows machines. These problems manifest when running *some* of the FUSION commands. The alternative would be to change all `LF` to `CR LF`.  

When possible, attempts were made to name variables using the same notation as the base scripts that are delivered with the FUSION installation.  

Verify that the file structure of your data is compatible with these scripts.  

## conda environment
- `cms2.yml` - YAML file used when developing these scripts. Note, this contains Spyder, which makes the environment bloated. Please review before installing.   

## lidar processing scripts  

### `scripts/AP`  
The FUSION Processing Scripts. Edit these at your own risk.  

### `scripts/01_PrepareDataForFusion.py`  
This script calls PDAL and FUSION.  
User needs to edit the following:  
- `project` - the name of the lidar project
- `dirLidarOriginal` - file path to the input lidar data
- `dirFUSION` - file path to FUSION executables
- `nCoresMax` - maximum number of processing cores available
- `dirBase` - main output directory

### `scripts/02_CreateAPSettingsPRP.R`  
This script calls FUSION.  
User needs to edit the following:  
- `SRS.Lidar` - Spatial refernce system of the lidar files
- `dirFUSION` - file path to FUSION executables
- `project` - the name of the lidar project
- `CELLSIZE` - Spatial resolution of the FUSION products
- `NCORESMAX` - maximum number of processing cores available
- `DIR_BASE` - main output directory
- `DIR_LIDAR` - directory of the lidar files projected in `01_PrepareDataForFusion.py`
- `DIRSCRIPTS` - directory of the FUSION AreaProcessor scripts (these scripts are found in `scripts/AP`)  

### `scripts/03_CreateGriddedMetrics.py`
This script calls FUSION.  
User needs to edit the following:  
- `project` - the name of the lidar project
- `dirBase` - main output directory
- `dirFinalProducts` - directory where the final products should be saved  


## Usage  
Setup workflow.  
Run `scripts/01_PrepareDataForFusion.py` and `scripts/02_CreateAPSettingsPRP.R`.  
The output of `scripts/02_CreateAPSettingsPRP.R` is a PRP file that is used to set up the FUSION processing run.  
Open the FUSION program `AreaProcessor.exe` and load the PRP file. Create the processing layout. Create the processing scripts.  
Run `scripts/03_CreateGriddedMetrics.py`. This script runs the batch file created in `[DIR_BASE]/[studyArea]/Processing/AP/APFusion.bat`, cleans the FUSION grids, and copies various products to a user-specified directory.

Note: There is an alternative script `scripts/01_PrepareDataForFusion_MultiProjects.py` that is designed to loop over multiple lidar projects. The advantage of this script is FUSION QAQC is run in parallel with one project per job. Users are welcome to alter the other scripts such that they loop over multiple projects.

