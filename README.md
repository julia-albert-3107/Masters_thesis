# Masters_thesis
This repository contains the code and the data collected in the scope of my master’s thesis “Programming a weather prediction scoring algorithm to decrease safety risks when entering a wind turbine”. 

## General Remarks
Apache NiFi does not create the required database tables.
The description of the data import process as well as all the data has been uploaded to the sciebo folder for this master's thesis. 

To run the code for the Weather Prediction Scoring Algorithm a PostgreSQL database needs to be running. This database has to contain the required database tables that include the data that has been imported by the integration system. The connection to the database is configured in the Python files and has to be adjusted.

## NiFi - Integration process
### Option 1: Importing the process groups
1. Download the four json files from the [NiFi Code folder](https://github.com/julia-albert-3107/Masters_thesis/tree/main/NiFi%20Code)
2. Start Apache NiFi
3. Drag the "Process Group" symbol in the top left into middle of the screen
4. Click on the "Browse" botton
5. Select one of the four files to import
6. Click "Add"
7. Repeat step 3 - 6 to add the other process groups

## Python data import - Integration Process
- the code to import the wave radar and met mast data from Nordsee Ost (NSO) and Gwynt-y-Mor (GyM) can be found in the folder [Python data import](https://github.com/julia-albert-3107/Masters_thesis/tree/main/Python%20data%20import)
- the code is include explanatory comments
- the location of the files might have to be adjusted in the code 

### Option 2: Import all data flows as NiFi templates
1. Download the xml files from the [NiFi Code folder](https://github.com/julia-albert-3107/Masters_thesis/tree/main/NiFi%20Code)
2. Start Apache NiFi
3. (Optional) Create a new process group by dragging the "Process Group" symbol in the top left into middle of the screen
4. (Optional) Name the process group and click "Add"
5. (Optional) Open the new, empty process group
6. Click on the "Upload template" symbol on the left
7. Click the "Browse" symbol
8. Select one of the xml files
9. Click "Upload"
10. Repeat step 3 or 6 - 9 to import the other data flows

## Weather Prediction Scoring Algorithm - Scoring of the data
The Weather Prediction Scoring Algorithm (WPSA) consists of three main files that calculate the three different scores. When the code is run, those files should be run in the following order: accuracyScore.py --> safetyRiskDetectionScore.py --> falsePositiveScore.py \
The code includes explanatory comments, so only some functions will be further explained in this document.

### Accuracy Scores ([accuracyScore.py](https://github.com/julia-albert-3107/Masters_thesis/blob/main/Weather%20Prediction%20Scoring%20Algorithm/accuracyScore.py))
Functions will be explained in the order they appear in in the code. When the file is executed, all Accuracy Score are calculated and inserted into the database.

- **calculateRecordParameterAccuracyScores(forecastProvider, forecasts, observations):** function to calculate the Accuracy Score for all parameters in a record according to equation (7.1) in my thesis
    - $AS(f,l,t,p) = |observation(f,t,l,p) - forecast(f,t,l,p)|$ 
    - it calls the binaryAccuracyScore(...) or differenceAccuracyScore(...) functions depending on the parameter
    - scores are calculated for all parameters, regardless of the technology (irrelevant parameters are ignored later on)
- **normalizeScores(forecastProvider, scores):** function that normalizes all parameter scores for an hourly forecast record according to equation (7.2) in my thesis
    - $AS'(f,l,t,p) = 
        \begin{cases}
            0, & \text{if } AS(f,l,t,p) > limit  \\
            0, & \text{if the forecast is missing} \\
            1 - (AS(f,l,t,p) / limit), & \text{if } AS(f,l,t,p) \le limit
        \end{cases} $
    - it calls the normalizeNumericScore(...) or normalizeBinaryScore(...) function depending on the parameter
- **totalRecordAccuracyOnshore(forecastProvider, scores)** function that calculates the total Accuracy Score for an hourly onshore forecast record according to equation (7.3) in my thesis
    - $AS_{record}(f,l,t) = \frac{\sum_{p \in P^{\ast}}^{}AS'(f,l,t,p)}{\sum_{p \in P^{\ast}}^{}1}$
    - only onshore weather parameters are considered (temperature, wind speed, snow, ice, lightning)
- **totalRecordAccuracyOffshore(forecastProvider, scores, relevantParameters):** function that calculates the total Accuracy Score for an hourly offshore forecast record according to equation (7.3) in my thesis 
    - $AS_{record}(f,l,t) = \frac{\sum_{p \in P^{\ast}}^{}AS'(f,l,t,p)}{\sum_{p \in P^{\ast}}^{}1}$
    - list of relevant parameters as input --> different parameters are relevant for some offshore sites
- **totalAccuracyScoresProviderLocation(forecastProvider, location, scores, scoreName, scoreType):** function that calculates the total parameter Accuracy Score and total Accuracy Score for a provider at a specific location according to equation (7.4) & (7.5) in my thesis
    - this function can be used for raw and normalized scores --> raw scores are not comparable but can be used for reference
    - the raw total score should not be calculated as it has almost no meaning (scores are within different ranges)
    - for the total score, the total record Accuracy Scores are treated like a parameter --> record total scores are calculated using only the relevant parameters
    - $AS_{total}(f,l) = \frac{\sum_{t \in T^{\ast}}^{}AS_{record}(f,l,t)}{\sum_{t \in T^{\ast}}^{}1}$
    - $AS_{parameter}(f,l,p) = \frac{\sum_{t \in T^{\ast}}^{}AS'(f,l,t,p)}{\sum_{t \in T^{\ast}}^{}1}$ 
    
### Safety Risk Detection Score [safetyRiskDetectionScore.py](https://github.com/julia-albert-3107/Masters_thesis/blob/main/Weather%20Prediction%20Scoring%20Algorithm/safetyRiskRetectionScore.py)
- the general and site-specific weather limits are included on top of the file --> those limits are used to detect safety risks depending on the location and technology
- **recordParameterSafetyRiskDetectionScoreOnshore(forecastProvider, forecasts, observations):** function to calculate the Safety Risk Detection Score for all parameters in an onshore record according to equation (7.6) in my thesis
    - $\text{if } \ observation(f,t,l,p) = \text{safety risk, } \forall p \in P^{\ast}: \\
    SRDS(f,t,l,p) = 
        \begin{cases}
            1, & \text{if } forecast(f,t,l,p) = \text{safety risk}  \\
            0, & \text{if } forecast(f,t,l,p) = \text{no safety risk} 
        \end{cases}$
    - this function calls the relevant functions for all parameters 
- **recordParameterSafetyRiskDetectionScoreOffshore(forecastProvider, forecasts, observations):** function to calculate the Safety Risk Detection Score for all parameters in an offshore record according to equation (7.6) in my thesis
    - $\text{if } \ observation(f,t,l,p) = \text{safety risk, } \forall p \in P^{\ast}: \\
    SRDS(f,t,l,p) = 
        \begin{cases}
            1, & \text{if } forecast(f,t,l,p) = \text{safety risk}  \\
            0, & \text{if } forecast(f,t,l,p) = \text{no safety risk} 
        \end{cases}$
    - this function calls the relevant functions for all parameters 
    - all offshore parameters are calculated; irrelevant parameters can be ignored later
- **parameterSafetyRiskDetectionScore(forecastProvider, location, scores, scoreName):** function to calculate the parameter total Safety Risk Detection Score for a provider at a location according to equation (7.7) in my thesis
    - $SRDS_{parameter}(f,l,p) = \frac{\sum_{t \in T^{\ast}}^{}SRDS(f,t,l,p)}{\sum_{t \in T^{\ast}}^{}1}$
- **totalSafetyRiskDetectionScore(forecastProvider, location, scores, relevantParameters):** function to calculate the total Safety Risk Detection Score for a provider at a location according to equation (7.8) in my thesis
    - $SRDS_{total}(f,l) = \frac{\sum_{p \in P^{\ast}}^{}\left(\sum_{t \in T^{\ast}}^{}SRDS(f,l,t,p)\right)}{\sum_{r \in OR(l)}^{}1}$
    - list of relevant parameters as input --> different parameters are relevant for different locations

### False Positive Score [falsePositiveScore.py](https://github.com/julia-albert-3107/Masters_thesis/blob/main/Weather%20Prediction%20Scoring%20Algorithm/falsePositiveScore.py)
- the general and site-specific weather limits are included on top of the file --> those limits are used to detect safety risks depending on the location and technology
- **recordFalsePositiveScoreOnshore(forecastProvider, forecasts, observations):** function to calculate the False Positive Score for all parameters in an onshore record according to equation (7.9) in my thesis
    - $if \ forecast(f,t,l,p) = \text{safety risk, } \forall p \in P^{\ast}: \\ 
        FPS(f,t,l,p) = 
        \begin{cases}
            0, & \text{if } observation(f,t,l,p) = \text{safety risk} \\
            1, & \text{if } observation(f,t,l,p) = \text{no safety risk}  
        \end{cases}$ 
    - this function calls the relevant functions for all parameters 
- **recordFalsePositiveScoreOffshore(forecastProvider, forecasts, observations):** function to calculate the False Positive Score for all parameters in an offshore record according to equation (7.9) in my thesis
    - $if \ forecast(f,t,l,p) = \text{safety risk, } \forall p \in P^{\ast}: \\ 
        FPS(f,t,l,p) = 
        \begin{cases}
            0, & \text{if } observation(f,t,l,p) = \text{safety risk} \\
            1, & \text{if } observation(f,t,l,p) = \text{no safety risk}  
        \end{cases}$ 
    - this function calls the relevant functions for all parameters 
- **parameterFalsePositiveScore(forecastProvider, location, scores, scoreName):** function to calculate the parameter total False Positive Score for a provider at a location according to equation (7.10) in my thesis
    - $FPS_{parameter}(f,l,p) = \frac{\sum_{t \in T^{\ast}}^{}FPS(f,t,l,p)}{\sum_{t \in T^{\ast}}^{}1}$
- **totalFalsePositiveScore(forecastProvider, location, scores, relevantParameters):** function to calculate the total False Positive Score for a provider at a location according to equation (7.11) in my thesis
    - $FPS_{total}(f,l) = \frac{\sum_{p \in P^{\ast}}^{}\left(\sum_{t \in T^{\ast}}^{}FPS(f,l,t,p)\right)}{\sum_{r \in FR(l)}^{}1}$
    - list of relevant parameters as input --> different parameters are relevant for different locations
