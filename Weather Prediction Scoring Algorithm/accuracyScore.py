import psycopg2
import psycopg2.extras
import time
import winsound

# connection to the database --> values have to be adjusted
hostname = 'hostname' 
database = 'databaseName'
username = 'postgres'
pwd = 'password'
port_id = 5432

conn = None

class Null:
    pass

try:
    print('Calculating...')

    # open DB connection
    with psycopg2.connect(
        host = hostname,
        dbname = database,
        user = username,
        password = pwd,
        port = port_id
    ) as conn:

        with conn.cursor(cursor_factory = psycopg2.extras.DictCursor) as cur:
            # normalization limits
            temperatureLimit = 10
            windSpeedLimit = 10
            windGustLimit = 10
            visibilityLimit = 1
            significantWaveHeightLimit = 1
            maximumWaveHeightLimit = 1

            # play notification sound when calculation is done --> this might only work on Windows
            def notify():
                duration = 1000  # milliseconds
                freq = 440  # Hz
                winsound.Beep(freq, duration)

            # select records from the database --> time frame = 2022
            def selectRecords(forecastProvider, location):
                if forecastProvider == 'Visual Crossing':
                    table = 'weather_forecasts_visual_crossing'
                elif forecastProvider == 'weather api':
                    table = 'weather_forecasts_weather_api'
                elif forecastProvider == 'World Weather Online':
                    table = 'weather_forecasts_world_weather_online'
                elif forecastProvider == 'World Weather Online Marine':
                    table = 'weather_forecasts_world_weather_online_marine'
                else:
                    print('Not a valid forecast provider! Valid providers are: "Visual Crossing", "weather api", '
                          '"World Weather Online", "World Weather Online Marine"')
                    return

                # forecasts
                cur.execute('''
                    SELECT * FROM {}
                    WHERE location_name = '{}'
                    AND prediction_date > '2021-12-31'
                    AND prediction_date < '2023-01-01'
                    '''.format(table, location))
                forecasts = cur.fetchall()

                # observations
                cur.execute('''
                    SELECT * FROM weather_data
                    WHERE location_name = '{}'
                    AND observation_date > '2021-12-31'
                    AND observation_date < '2023-01-01'
                    '''.format(location))
                observations = cur.fetchall()

                return forecasts, observations

            # parameter score for binary parameters (snow, ice, lightning)
            def binaryAccuracyScore(forecast, observation):
                if observation is None:  # ignore score if no observation --> 999 = invalid flag
                    score = 999
                elif forecast is None:  # worst score if no forecast --> no forecast = wrong forecast
                    score = 1
                else:
                    if forecast == observation:
                        score = 0
                    else:
                        score = 1

                return score

            # parameter score for non-binary parameters (wind speed, wind gusts, temperature, visibility, significant &
            # maximum wave height))
            def differenceAccuracyScore(forecast, observation):
                if observation is None:  # ignore score if no observation --> 999 = invalid flag
                    score = 999
                elif forecast is None:  # temporary fixed score --> will be adjusted to worst score during normalization --> 1000 = no forecast flag
                    score = 1000
                else:
                    score = abs(observation - forecast)

                return score

            # accuracy score for all parameters in a record --> equation (7.1) in my thesis
            def calculateRecordParameterAccuracyScores(forecastProvider, forecasts, observations):
                if forecastProvider == 'Visual Crossing':
                    table = 'raw_scores_visual_crossing'
                    constraint = 'unique_timestamp_visual_crossing_raw_score'
                elif forecastProvider == 'weather api':
                    table = 'raw_scores_weather_api'
                    constraint = 'unique_timestamp_weather_api_raw_score'
                elif forecastProvider == 'World Weather Online':
                    table = 'raw_scores_world_weather_online'
                    constraint = 'unique_timestamp_world_weather_online_raw_score'
                elif forecastProvider == 'World Weather Online Marine':
                    table = 'raw_scores_world_weather_online_marine'
                    constraint = 'unique_timestamp_world_weather_online_marine_raw_score'
                else:
                    print('Not a valid forecast provider! Valid providers are: "Visual Crossing", "weather api", '
                          '"World Weather Online", "World Weather Online Marine"')
                    return

                for f in forecasts:
                    for o in observations:
                        if f['prediction_date'] == o['observation_date'] and f['prediction_time'] == \
                                o['observation_time']:
                            windspeedScore = differenceAccuracyScore(f['windspeed_m_s'], o['windspeed_m_s'])
                            temperatureScore = differenceAccuracyScore(f['temperature_c'], o['temperature_c'])
                            snowScore = binaryAccuracyScore(f['snow'], o['snow'])
                            iceScore = binaryAccuracyScore(f['ice'], o['ice'])
                            lightningScore = binaryAccuracyScore(f['lightning'], o['lightning'])
                            windgustScore = differenceAccuracyScore(f['windgusts_m_s'], o['windgusts_m_s'])
                            visibilityScore = differenceAccuracyScore(f['visibility_km'], o['visibility_km'])
                            significantWaveHeightScore = differenceAccuracyScore\
                                (f['significant_wave_height_m'], o['significant_wave_height_m'])
                            maximumWaveHeightScore = differenceAccuracyScore(f['maximum_wave_height_m'],
                                                                                       o['maximum_wave_height_m'])

                            cur.execute('''
                                INSERT INTO {} (prediction_date, prediction_time, location_name, 
                                windspeed_score, temperature_score, snow_score, ice_score, lightning_score, 
                                windgust_score, visibility_score, significant_wave_height_score, 
                                maximum_wave_height_score)
                                VALUES('{}', '{}', '{}', {}, {}, {}, {}, {}, {}, {}, {}, {})
                                ON CONFLICT ON CONSTRAINT {} DO UPDATE
                                SET  windspeed_score = {}, temperature_score = {}, snow_score = {}, ice_score = {},
                                lightning_score = {}, windgust_score = {}, visibility_score = {}, 
                                significant_wave_height_score = {}, maximum_wave_height_score = {}
                                '''.format(table, f['prediction_date'], f['prediction_time'], f['location_name'],
                                        windspeedScore, temperatureScore, snowScore, iceScore, lightningScore,
                                        windgustScore, visibilityScore, significantWaveHeightScore,
                                        maximumWaveHeightScore, constraint, windspeedScore, temperatureScore, snowScore,
                                           iceScore, lightningScore, windgustScore, visibilityScore,
                                           significantWaveHeightScore, maximumWaveHeightScore))

            # normalization --> equation (7.2) in my thesis
            def normalizeNumericScore(score, limit):
                if score == 1000:  # no forecast available --> return the worst possible score (0)
                    return 0
                elif score == 999:  # no observation available --> ignore score and return 999
                    return 999
                elif score > limit:  # score very large (bad) --> forecast is too inaccurate --> return the worst
                    # possible score (0)
                    return 0
                else:  # divide values by maximum (limit) & "invert" score (1 - score) --> 1 = high accuracy,
                    # 0 = low accuracy
                    normalizedScore = 1 - (score / limit)
                    return normalizedScore

            def normalizeBinaryScore(score):
                if score == 999:  # no observation available --> ignore score and return 999
                    return 999
                else:
                    return 1 - score  # 'invert' score --> 0 = low accuracy, 1 = high accuracy

            # function to select normalized or raw scores
            def selectScores(forecastProvider, location, scoreType):
                if scoreType == 'Normalized':
                    if forecastProvider == 'Visual Crossing':
                        table = 'normalized_scores_visual_crossing'
                    elif forecastProvider == 'weather api':
                        table = 'normalized_scores_weather_api'
                    elif forecastProvider == 'World Weather Online':
                        table = 'normalized_scores_world_weather_online'
                    elif forecastProvider == 'World Weather Online Marine':
                        table = 'normalized_scores_world_weather_online_marine'
                    else:
                        print('Not a valid forecast provider! Valid providers are: "Visual Crossing", "weather api", '
                              '"World Weather Online", "World Weather Online Marine"')
                        return
                elif scoreType == 'Raw':
                    if forecastProvider == 'Visual Crossing':
                        table = 'raw_scores_visual_crossing'
                    elif forecastProvider == 'weather api':
                        table = 'raw_scores_weather_api'
                    elif forecastProvider == 'World Weather Online':
                        table = 'raw_scores_world_weather_online'
                    elif forecastProvider == 'World Weather Online Marine':
                        table = 'raw_scores_world_weather_online_marine'
                    else:
                        print('Not a valid forecast provider! Valid providers are: "Visual Crossing", "weather api", '
                              '"World Weather Online", "World Weather Online Marine"')
                        return

                query = '''
                    SELECT * FROM {}
                    WHERE location_name = '{}'
                    AND prediction_date > '2021-12-31'
                    AND prediction_date < '2023-01-01'
                    '''.format(table, location)
                cur.execute(query)
                return cur.fetchall()

            # normalize all scores (regardless of whether they are relevant for that technology for not)
            def normalizeScores(forecastProvider, scores):
                if forecastProvider == 'Visual Crossing':
                    table = 'normalized_scores_visual_crossing'
                    constraint = 'unique_timestamp_visual_crossing_normalized_score'
                elif forecastProvider == 'weather api':
                    table = 'normalized_scores_weather_api'
                    constraint = 'unique_timestamp_weather_api_normalized_score'
                elif forecastProvider == 'World Weather Online':
                    table = 'normalized_scores_world_weather_online'
                    constraint = 'unique_timestamp_world_weather_online_normalized_score'
                elif forecastProvider == 'World Weather Online Marine':
                    table = 'normalized_scores_world_weather_online_marine'
                    constraint = 'unique_timestamp_world_weather_online_marine_normalized_score'
                else:
                    print('Not a valid forecast provider! Valid providers are: "Visual Crossing", "weather api", '
                          '"World Weather Online", "World Weather Online Marine"')
                    return

                for s in scores:
                    windspeedScore = normalizeNumericScore(s['windspeed_score'], windSpeedLimit)
                    windgustScore = normalizeNumericScore(s['windgust_score'], windGustLimit)
                    temperatureScore = normalizeNumericScore(s['temperature_score'], temperatureLimit)
                    visibilityScore = normalizeNumericScore(s['visibility_score'], visibilityLimit)
                    significantWaveHeightScore = normalizeNumericScore(s['significant_wave_height_score'],
                                                                       significantWaveHeightLimit)
                    maximumWaveHeightScore = normalizeNumericScore(s['maximum_wave_height_score'],
                                                                   maximumWaveHeightLimit)
                    snowScore = normalizeBinaryScore(s['snow_score'])
                    iceScore = normalizeBinaryScore(s['ice_score'])
                    lightningScore = normalizeBinaryScore(s['lightning_score'])

                    cur.execute('''
                        INSERT INTO {} (prediction_date, prediction_time, location_name, 
                        windspeed_score, temperature_score, snow_score, ice_score, lightning_score, 
                        windgust_score, visibility_score, significant_wave_height_score, 
                        maximum_wave_height_score)
                        VALUES('{}', '{}', '{}', {}, {}, {}, {}, {}, {}, {}, {}, {})
                        ON CONFLICT ON CONSTRAINT {} DO UPDATE
                        SET prediction_date = '{}', prediction_time = '{}', location_name = '{}', 
                        windspeed_score = {}, temperature_score = {}, snow_score = {}, ice_score = {},
                        lightning_score = {}, windgust_score = {}, visibility_score = {}, 
                        significant_wave_height_score = {}, maximum_wave_height_score = {}
                        '''.format(table, s['prediction_date'], s['prediction_time'], s['location_name'],
                                   windspeedScore, temperatureScore, snowScore, iceScore, lightningScore,
                                   windgustScore, visibilityScore, significantWaveHeightScore, maximumWaveHeightScore,
                                   constraint, s['prediction_date'], s['prediction_time'], s['location_name'],
                                   windspeedScore, temperatureScore, snowScore, iceScore, lightningScore,
                                   windgustScore, visibilityScore, significantWaveHeightScore, maximumWaveHeightScore))

            # calculate the total accuracy score for an hourly onshore forecast record --> equation (7.3) in my thesis
            def totalRecordAccuracyOnshore(forecastProvider, scores):
                # all onshore wind farms only have a single weather station as the observation source
                if forecastProvider == 'Visual Crossing':
                    table = 'normalized_scores_visual_crossing'
                elif forecastProvider == 'weather api':
                    table = 'normalized_scores_weather_api'
                elif forecastProvider == 'World Weather Online':
                    table = 'normalized_scores_world_weather_online'
                elif forecastProvider == 'World Weather Online Marine':
                    table = 'normalized_scores_world_weather_online_marine'
                else:
                    print('Not a valid forecast provider! Valid providers are: "Visual Crossing", "weather api", '
                          '"World Weather Online", "World Weather Online Marine"')
                    return

                # relevant onshore parameters: snow, ice, lightning, temperature, wind speed
                for s in scores:
                    # if a parameter score is 999 (no observation available) the total score is also invalid (999)
                    if s['snow_score'] == 999 or s['ice_score'] == 999 or s['lightning_score'] == 999 or \
                            s['temperature_score'] == 999 or s['windspeed_score'] == 999:
                        score = 999
                    else:
                        scoreSum = s['snow_score'] + s['ice_score'] + s['lightning_score'] + s['temperature_score'] + \
                               s['windspeed_score']
                        score = scoreSum / 5  # sum divided by number of relevant parameters

                    cur.execute('''
                        UPDATE {}
                        SET total_score = {}
                        WHERE prediction_date = '{}' AND prediction_time = '{}' AND location_name = '{}'
                        '''.format(table, score, s['prediction_date'], s['prediction_time'], s['location_name']))

            # calculate the total accuracy score for an hourly offshore forecast record --> equation (7.3) in my thesis
            # relevant parameters vary for offshore sites --> list of relevant parameters as an input
            def totalRecordAccuracyOffshore(forecastProvider, scores, relevantParameters):
                if forecastProvider == 'Visual Crossing':
                    table = 'normalized_scores_visual_crossing'
                elif forecastProvider == 'weather api':
                    table = 'normalized_scores_weather_api'
                elif forecastProvider == 'World Weather Online':
                    table = 'normalized_scores_world_weather_online'
                elif forecastProvider == 'World Weather Online Marine':
                    table = 'normalized_scores_world_weather_online_marine'
                else:
                    print('Not a valid forecast provider! Valid providers are: "Visual Crossing", "weather api", '
                          '"World Weather Online", "World Weather Online Marine"')
                    return

                # relevant offshore parameters (general): snow, ice, lightning, temperature, wind speed, wind gusts,
                # visibility, significant wave height, maximum wave height
                # Maximum wave height is not relevant for Nordsee Ost --> not in the custom relevant parameter list
                for s in scores:
                    invalid = False
                    scoreSum = 0
                    # if a parameter score is 999 (no observation available) the total score is also invalid (999)
                    for p in relevantParameters:
                        if s[p] == 999:
                            invalid = True
                    if invalid:
                        score = 999
                    else:
                        for p in relevantParameters:
                            scoreSum += s[p]
                        score = scoreSum / len(relevantParameters)  # sum divided by number of relevant parameters

                    cur.execute('''
                          UPDATE {}
                          SET total_score = {}
                           WHERE prediction_date = '{}' AND prediction_time = '{}' AND location_name = '{}'
                           '''.format(table, score, s['prediction_date'], s['prediction_time'],
                                      s['location_name']))

            # total parameter accuracy score and total accuracy score for a provider at a specific location --> equation (7.4) & (7.5) in my thesis
            # this function can be used for raw and normalized scores --> raw scores are not comparable but can be used for reference
            def totalAccuracyScoresProviderLocation(forecastProvider, location, scores, scoreName, scoreType):
                # record total scores are calculated using only the relevant parameters --> can use total record
                # scores to calculate the total scores for a provider and location
                if scoreType == 'Normalized':
                    score_type = 'Accuracy Score'
                elif scoreType == 'Raw':
                    score_type = 'Raw Accuracy Score'
                else:
                    print('Error: Not a valid score type! (Valid score types are "Normalized" and "Raw")')
                    return

                totalScoreSum = 0  # variable that stores the current total score sum
                numberOfRecords = 0  # variables that stores the number of considered records (not 999 scores)

                for s in scores:
                    if s[scoreName] is None:
                        print('Error: Not all total scores have been calculated!')
                        return
                    elif s[scoreName] == 999:  # score = 999 --> invalid (observation missing) --> ignore
                        continue
                    elif s[scoreName] == 1000 and scoreType == 'Raw':  # ignore missing forecasts for raw score total
                        # (score cannot be 1000 for normalized scores --> fixed during normalization)
                        continue
                    else:
                        totalScoreSum += s[scoreName]  # add score to current total
                        numberOfRecords += 1  # increment number of records

                if numberOfRecords == 0:  # all records have invalid total scores
                    totalScore = 999
                else:
                    totalScore = totalScoreSum / numberOfRecords

                cur.execute('''
                    INSERT INTO scores (location_name, forecast_source, score_type, {})
                    VALUES ('{}', '{}', '{}', {})
                    ON CONFLICT ON CONSTRAINT unique_source_and_score_type_per_location DO UPDATE
                    SET {} = {}
                    '''.format(scoreName, location, forecastProvider, score_type, totalScore, scoreName, totalScore))

            # lists of providers and locations
            locations = ('Putlitz', 'Lindhurst', 'Barzowice', 'Nordsee Ost Merged', 'Gwynt-y-Mor Merged')
            onshoreLocations = ('Putlitz', 'Lindhurst', 'Barzowice')
            offshoreLocations = ('Nordsee Ost Merged', 'Gwynt-y-Mor Merged')

            # list of forecast providers
            forecastProviders = ('Visual Crossing', 'weather api', 'World Weather Online', 'World Weather Online Marine'
                                )

            # list of relevant parameters depending on the technology
            onshoreScores = ('snow_score', 'ice_score', 'lightning_score', 'temperature_score', 'windspeed_score')
            offshoreScores = ('snow_score', 'ice_score', 'lightning_score', 'temperature_score', 'windspeed_score',
                              'windgust_score', 'visibility_score', 'significant_wave_height_score',
                              'maximum_wave_height_score')
            nordseeOstScores = ('snow_score', 'ice_score', 'lightning_score', 'temperature_score', 'windspeed_score',
                                'windgust_score', 'visibility_score', 'significant_wave_height_score')

            # Parameter lists without visibility --> observations seem rounded --> scores might be invalid
            offshoreScoresNoVisibility = ('snow_score', 'ice_score', 'lightning_score', 'temperature_score',
                                          'windspeed_score', 'windgust_score', 'significant_wave_height_score',
                                          'maximum_wave_height_score')
            nordseeOstScoresNoVisibility = ('snow_score', 'ice_score', 'lightning_score', 'temperature_score',
                                            'windspeed_score', 'windgust_score', 'significant_wave_height_score')

            # list of score types
            scoreTypes = ('Raw', 'Normalized')

            # calculate parameter score for all provider and location combinations regardless of the technology
            # irrelevant parameter scores will be ignored during the total score calculation
            # non existed provider and location combinations (Putlitz x marine forecast provider will be automatically
            # ignored (will only produce invalid scores)

            for location in locations:
                for provider in forecastProviders:
                    start_time = time.time()  # start code timer
                    forecasts, observations = selectRecords(provider, location)
                    calculateRecordParameterAccuracyScores(provider, forecasts, observations)
                    time_score = time.time()
                    print(provider, location, 'is done (', time_score - start_time, 'seconds)')  # print status message
            
            # normalize raw scores
            for location in locations:
                for provider in forecastProviders:
                    scores = selectScores(provider, location, 'Raw')
                    normalizeScores(provider, scores)
                    print(provider, location, 'is done')
            
            # calculate total normalized accuracy score per record depending on the technology
            # Onshore
            for location in onshoreLocations:
                for provider in forecastProviders:
                    start_time = time.time()  # start code timer
                    scores = selectScores(provider, location, 'Normalized')
                    totalRecordAccuracyOnshore(provider, scores)
                    time_score = time.time()
                    print('Total scores per record for', provider, location, 'are done (', time_score - start_time,
                          'seconds)')  # print status message

            # Offshore - Nordsee Ost has custom relevant parameters (maximum wave height irrelevant)
            for provider in forecastProviders:
                start_time = time.time()  # start code timer
                scores = selectScores(provider, 'Nordsee Ost Merged', 'Normalized')
                totalRecordAccuracyOffshore(provider, scores, nordseeOstScores)
                time_score = time.time()
                print('Total scores per record for', provider, 'Nordsee Ost Merged are done (', time_score - start_time,
                      'seconds)')  # print status message
            
            # Gwynt-y-Mor (normal parameter list)
            for provider in forecastProviders:
                start_time = time.time()  # start code timer
                scores = selectScores(provider, 'Gwynt-y-Mor Merged', 'Normalized')
                totalRecordAccuracyOffshore(provider, scores, offshoreScores)
                time_score = time.time()
                print('Total scores per record for', provider, 'Gwynt-y-Mor Merged are done (', time_score - start_time,
                      'seconds)')  # print status message

            # calculate normalized and raw parameter accuracy scores per score type, provider & location depending on
            # the technology
            # Onshore
            for location in onshoreLocations:
                for provider in forecastProviders:
                    for scoreType in scoreTypes:
                        scores = selectScores(provider, location, scoreType)
                        for scoreName in onshoreScores:
                            start_time = time.time()  # start code timer
                            # calculate total scores for all relevant parameters
                            totalAccuracyScoresProviderLocation(provider, location, scores, scoreName, scoreType)
                            time_score = time.time()
                        if scoreType == 'Normalized':  # calculate the normalized total score
                            start_time = time.time()  # start code timer
                            totalAccuracyScoresProviderLocation(provider, location, scores, 'total_score', scoreType)
                            time_score = time.time()
                    print('Total and parameter scores for', provider, location, 'are done (', time_score -
                          start_time, 'seconds)')  # print status message
            
            # Offshore
            for location in offshoreLocations:
                for provider in forecastProviders:
                    for scoreType in scoreTypes:
                        scores = selectScores(provider, location, scoreType)
                        for scoreName in offshoreScores:
                            start_time = time.time()  # start code timer
                            # calculate total scores for all relevant parameters
                            totalAccuracyScoresProviderLocation(provider, location, scores, scoreName, scoreType)
                            time_score = time.time()
                        if scoreType == 'Normalized':  # calculate the normalized total score
                            start_time = time.time()  # start code timer
                            totalAccuracyScoresProviderLocation(provider, location, scores, 'total_score', scoreType)
                            time_score = time.time()
                    print('Total and parameter scores for', provider, location, 'are done (', time_score -
                          start_time, 'seconds)')  # print status message

            notify()  # notify user when calculations are done --> this calculation can take a while

except Exception as error:
    print(error)
finally:
    # close DB connection
    if conn is not None:
        conn.close()
