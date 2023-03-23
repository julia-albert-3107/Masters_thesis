# Masters_thesis
This repository will contain the code and the data collected in the scope of my master’s thesis “Programming a weather prediction scoring algorithm to decrease safety risks when entering a wind turbine”. It will be updated within the following days.

## General Remarks
Apache NiFi does not create the required database tables.
> **Note:**
> PostgreSQL scripts to create the required tables will be uploaded soon.

To run the code for the Weather Prediction Scoring Algorithm a PostgreSQL database needs to be running. This database has to contain the required database tables that include the data that has been imported by the integration system. The connection to the database is configured in the Python files and has to be adjusted.

## NiFi - Intergration process
> **Note:**
> Work in progress

## Weather Prediction Scoring Algorithm - Scoring of the data
The Weather Prediction Scoring Algorithm (WPSA) consists of three main files that calculate the three different scores. When the code is run, those files should be run in the following order: accuracyScore.py --> safetyRiskDetectionScore.py --> falsePositiveScore.py \
The code includes explanatory comments.

### Accuracy Scores ([accuracyScore.py](https://github.com/julia-albert-3107/Masters_thesis/blob/main/Weather%20Prediction%20Scoring%20Algorithm/accuracyScore.py))
Functions will be explained in the order they appear in in the code. When the file is executed, all Accuracy Score are calculated and inserted into the database.

- **notify():** function that plays a sound --> used to notify the user that the calculations are done (running this file took about 30 minutes for me)
- **selectRecords(forecastProvider, location):** function that selects all hourly forecast and observation records for a given provider and location
- **binaryAccuracyScore(forecast, observation):** function that calculates the accuracy score for binary parameters (snow, ice, lightning)
    - this function is called by the calculateRecordParameterAccuracyScores(...) function
    - input: a single hourly forecast parameter value and the corresponding observation value 
- **differenceAccuracyScore(forecast, observation):** function that calculates the accuracy score for numeric parameters (temperature, wind speed, wind gusts, visibility, significant wave height, maximum wave height)
    - this function is called by the calculateRecordParameterAccuracyScores(...) function
    - input: a single hourly forecast parameter value and the corresponding observation value 
- **calculateRecordParameterAccuracyScores(forecastProvider, forecasts, observations):** function to calculate the accuracy score for all parameters in a record according to equation (7.1) in my thesis
    - $AS(f,l,t,p) = |observation(f,t,l,p) - forecast(f,t,l,p)|$ 
    - it calls the binaryAccuracyScore(...) or differenceAccuracyScore(...) functions depending on the parameter
    - scores are calculated for all parameters, regardless of the technology (irrelevant parameters are ignored later on)
- **normalizeNumericScore(score, limit):** function that normalizes numeric scores using the normalization limits that are set in the code 
    - this function is called by the normalizeScores(...) function
- **normalizeBinaryScore(score):** function that normalizes binary scores 
- **selectScores(forecastProvider, location, scoreType):** function to select raw or normalized scores from the database
    - this function is called by the normalizeScores(...) function
- **normalizeScores(forecastProvider, scores):** function that normalizes all parameter scores for an hourly forecast record according to equation (7.2) in my thesis
    - $AS'(f,l,t,p) = 
        \begin{cases}
            0, & \text{if } AS(f,l,t,p) > limit  \\
            0, & \text{if the forecast is missing} \\
            1 - (AS(f,l,t,p) / limit), & \text{if } AS(f,l,t,p) \le limit
        \end{cases} $
    - it calls the normalizeNumericScore(...) or normalizeBinaryScore(...) function depending on the parameter
- **totalRecordAccuracyOnshore(forecastProvider, scores)** function that calculates the total accuracy score for an hourly onshore forecast record according to equation (7.3) in my thesis
    - $AS_{record}(f,l,t) = \frac{\sum_{p \in P^{\ast}}^{}AS'(f,l,t,p)}{\sum_{p \in P^{\ast}}^{}1}$
    - only onshore weather parameters are considered (temperature, wind speed, snow, ice, lightning)
- **totalRecordAccuracyOffshore(forecastProvider, scores, relevantParameters):** function that calculates the total accuracy score for an hourly offshore forecast record according to equation (7.3) in my thesis 
    - $AS_{record}(f,l,t) = \frac{\sum_{p \in P^{\ast}}^{}AS'(f,l,t,p)}{\sum_{p \in P^{\ast}}^{}1}$
    - list of relevant parameters as input --> different parameters are relevant for some offshore sites
- **totalAccuracyScoresProviderLocation(forecastProvider, location, scores, scoreName, scoreType):** function that calculates the total parameter accuracy score and total accuracy score for a provider at a specific location according to equation (7.4) & (7.5) in my thesis
    - this function can be used for raw and normalized scores --> raw scores are not comparable but can be used for reference 
    - for the total score, the total record accuracy scores are treated like a parameter --> record total scores are calculated using only the relevant parameters
    - $AS_{total}(f,l) = \frac{\sum_{t \in T^{\ast}}^{}AS_{record}(f,l,t)}{\sum_{t \in T^{\ast}}^{}1}$
    - $AS_{parameter}(f,l,p) = \frac{\sum_{t \in T^{\ast}}^{}AS'(f,l,t,p)}{\sum_{t \in T^{\ast}}^{}1}$ 
    
### Safety Risk Detection Score [safetyRiskDetectionScore.py](https://github.com/julia-albert-3107/Masters_thesis/blob/main/Weather%20Prediction%20Scoring%20Algorithm/safetyRiskRetectionScore.py)
> **Note:**
> Work in progress

### False Positive Score [falsePositiveScore.py](https://github.com/julia-albert-3107/Masters_thesis/blob/main/Weather%20Prediction%20Scoring%20Algorithm/falsePositiveScore.py)
> **Note:**
> Work in progress
