# Gaming_Behavior_Analysis

## Project Overview:

This project analyzes gaming behavior by combining player information and gaming activity data. The analysis involves data cleaning, joining, transformation, and visualization using Azure Data Factory (ADF) and Azure Databricks.

## Data Sources:

1. **raw-data1:** Contains player information.
    - `PlayerID`: string
    - `Age`: string
    - `Gender`: string
    - `Location`: string

2. **raw-data2**: Contains gaming behavior.
    - `PlayerID`: string
    - `GameGenre`: string
    - `PlayTimeHours`: string
    - `GameDifficulty`: string
    - `SessionsPerWeek`: string
    - `AvgSessionDurationMinutes`: string
    - `PlayerLevel`: string
    - `AchievementsUnlocked`: string
  
## Project Structure

- **Pipeline 1**:
  - Copy activities to move data from `raw-data1` and `raw-data2` to `temp-storage`.
  - Databricks notebook for data cleaning and joining.

- **Pipeline 2**:
  - Executes Pipeline 1.
  - Databricks notebook for data transformation.

## Data Cleaning and Joining

Performed in Databricks:
- **Cleaning**:
  - By Dropping any Duplicate datasets
  - By doing the necessary Data Type conversion
  - Handling NULL values with a default value or strategy (mean)
    
- **Joining**:
  - Joining datasets on `PlayerID`.

Result of this is stored as `.csv` file in the `refined-data` container of ADLS.

## Data Transformation

Performed in Databricks using PySpark:
- Calculating average `PlayTimeHours` by `GameGenre`.
- Average `PlayTimeHours` and `SessionsPerWeek` by `Gender`.
- Counting players by `GameDifficulty`.
- Finding maximum and minimum `PlayTimeHours` by `GameGenre`.
- `AvgSessionDurationMinutes` by `Gender`.
- Identifying top 10 players with highest `PlayTimeHours`.

## Results Storage

Results of transformations are stored as `.csv` files in the `transformed-data` container of ADLS.

## Azure Setup

1. **Create Azure Data Factory and Azure Databricks instances.**
2. **Set up Azure DataLake Gen2 Storage with containers:**
   - `raw-data1`
   - `raw-data2`
   - `temp-storage`
   - `refined-data`
   - `transformed-data`

## Configure ADF Pipelines

1. **Create a pipeline in ADF with copy activities to move data to `temp-storage`.**
2. **Integrate Databricks notebooks for data cleaning, joining, and transformation.**

## Run Pipelines

1. **Execute Pipeline 1 for data cleaning and joining.**
2. **Execute Pipeline 2 for data transformation and result storage.**

## Visualizations

**Visualizations are performed in Databricks notebooks and can be extended for more in-depth analysis.**

## Contact

For any inquiries, please contact [aishwaryareddy2920@gmail.com](mailto:aishwaryareddy2920@gmail.com).
