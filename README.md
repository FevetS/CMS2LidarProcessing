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

### `scripts/config.json`
Edit this JSON to define the paths and processing parameters to use across the three processing scripts. The specific parameters and how they're used in the script details below.

### `scripts/01_PrepareDataForFusion.py`  
This script calls PDAL and FUSION.  
User needs to edit the following in the config.json:  
- `project` - the name of the lidar project
- `dirInputBase` - base folder path to the input lidar data. All LAZ files should be in `[dirInputBase]\\project\\Points\\LAZ`
- `dirFUSION` - folder path to FUSION executables
- `nCoresMax` - maximum number of processing cores available
- `dirBase` - main output directory of preparations
- `srsIn` - string with input horizontal and optionally vertical EPSG code(s) of data, e.g. "6428" or "6428+6360". Use *null* if the spatial reference is correctly defined in the LAZ header.

### `scripts/02_CreateAPSettingsPRP.R`  
This script calls FUSION.  
User needs to edit the following in the config.json:  
- `dirFUSION` - file path to FUSION executables
- `project` - the name of the lidar project
- `CELLSIZE` - Spatial resolution of the FUSION products
- `nCoresMax` - maximum number of processing cores available
- `dirBase` - main output directory
- `DIRSCRIPTS` - directory of the FUSION AreaProcessor scripts (these scripts are found in `scripts/AP`)  

### `scripts/03_CreateGriddedMetrics.py`
This script calls FUSION.  
User needs to edit the following:  
- `project` - the name of the lidar project
- `dirBase` - main output directory
- `dirFinalProducts` - base directory where the final products should be saved  


## Usage  
Setup workflow.  
Edit the config.json with the correct paths and parameters. Call all scripts from the command line with the path to the config.json as the first argument (e.g., `python 01_PrepareDataForFusion.py C:\\path\\to\\config.json` or `Rscript.exe 02_CreateAPSettingsPRP.R C:\\path\\to\\config.json`).
Run `scripts/01_PrepareDataForFusion.py`, which will output a copy of the files in EPSG:5070 along with a QAQC of the data and file list to be used by the next script.
Run `scripts/02_CreateAPSettingsPRP.R` which outputs is a PRP file that is used to set up the FUSION processing run.  
Open the FUSION program `AreaProcessor.exe` and load the PRP file. Create the processing layout. Create the processing scripts.  
Run `scripts/03_CreateGriddedMetrics.py`. This script runs the batch file created in `[DIR_BASE]/[studyArea]/Processing/AP/APFusion.bat`, cleans the FUSION grids, and copies various products to a user-specified directory.

Note: There is an alternative script `scripts/01_PrepareDataForFusion_MultiProjects.py` that is designed to loop over multiple lidar projects. The advantage of this script is FUSION QAQC is run in parallel with one project per job. To use this script edit the config.json to provide a list of projects (e.g. ["CO_TellerCo_2016", "CO_Aspen_2020"], and a corresponding list of srsIn can also be provided (e.g. ["6428+6360", null]), but if null then the SRS will be read from the LAZ header for all projects.
Users are welcome to alter the other scripts such that they loop over multiple projects.

