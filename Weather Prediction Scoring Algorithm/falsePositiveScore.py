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
    # open DB connection
    with psycopg2.connect(
            host=hostname,
            dbname=database,
            user=username,
            password=pwd,
            port=port_id
    ) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # weather limits
            # onshore
            onshore_limit_low_temperature = -20
            onshore_limit_high_temperature = 43
            onshore_limit_windspeed_hub = 15
            onshore_limit_windspeed_nacelle = 20
            onshore_limit_windspeed_wind_farm = 25

            # offshore
            # general limits
            offshore_limit_high_temperature = 43
            offshore_limit_low_temperature = -15
            offshore_limit_windspeed_wingusts_crane = 15  # same limits for wind speed and wind gusts
            offshore_limit_significant_wave_height = 1.5
            offshore_limit_maximum_wave_height = 2.2
            offshore_limit_visibility = 0.05

            # specific limits Nordsee One (only if different from general limits) --> company guidelines
            # Note: maximum wave height irrelevant (according to official company guidelines)
            # "lowest" limit for wind turbine crane with normal load
            offshore_limit_windspeed_wingusts_crane_Nordsee_Ost = 13  # same limits for wind speed and wind gusts
            offshore_limit_significant_wave_height_Nordsee_Ost = 1.6

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

            # select false positive scores from the database
            def selectScores(forecastProvider, location):
                if forecastProvider == 'Visual Crossing':
                    table = 'false_positive_score_visual_crossing'
                elif forecastProvider == 'weather api':
                    table = 'false_positive_score_weather_api'
                elif forecastProvider == 'World Weather Online':
                    table = 'false_positive_score_world_weather_online'
                elif forecastProvider == 'World Weather Online Marine':
                    table = 'false_positive_score_world_weather_online_marine'
                else:
                    print('Not a valid forecast provider! Valid providers are: "Visual Crossing", "weather api", '
                          '"World Weather Online", "World Weather Online Marine"')
                    return

                cur.execute('''
                    SELECT * FROM {}
                    WHERE location_name = '{}'
                    AND prediction_date > '2021-12-31'
                    AND prediction_date < '2023-01-01'
                    '''.format(table, location))
                return cur.fetchall()

            # play notification sound when calculation is done --> this might only work on Windows
            def notify():
                duration = 1000  # milliseconds
                freq = 440  # Hz
                winsound.Beep(freq, duration)

            def onshoreTemperatureScore(forecast, observation):
                if forecast is None or observation is None:  # no forecast or observation --> invalid score
                    score = 999
                elif forecast < onshore_limit_low_temperature or forecast > onshore_limit_high_temperature:
                    # forecast = risk
                    if observation < onshore_limit_low_temperature or observation > onshore_limit_high_temperature:
                        # observation = risk
                        score = 0
                    else:  # observation = no risk
                        score = 1
                else:  # no risk predicted --> flag '2' for no risk
                    score = 2

                return score

            def offshoreTemperatureScore(forecast, observation):
                if observation is None or forecast is None:  # no forecast or observation --> invalid score
                    score = 999
                # forecast = risk
                elif forecast < offshore_limit_low_temperature or forecast > offshore_limit_high_temperature:
                    # observation = risk
                    if observation < offshore_limit_low_temperature or observation > offshore_limit_high_temperature:
                        score = 0
                    else:  # observation = no risk
                        score = 1
                else:  # no risk predicted --> flag '2' for no risk
                    score = 2

                return score

            def offshoreWindScore(forecast, observation, location):
                # chose limits based on location
                if 'Nordsee Ost' in location:
                    limit = offshore_limit_windspeed_wingusts_crane_Nordsee_Ost
                else:
                    limit = offshore_limit_windspeed_wingusts_crane

                if forecast is None or observation is None:  # no forecast or observation --> invalid score
                    score = 999
                elif forecast > limit:  # forecast = risk
                    if observation > limit:  # observation = risk
                        score = 0
                    else:  # observation = no risk
                        score = 1
                else:  # no risk predicted --> flag '2' for no risk
                    score = 2

                return score

            def offshoreVisibilityScore(forecast, observation):
                if observation is None or forecast is None:  # no forecast or observation --> invalid score
                    score = 999
                elif forecast < offshore_limit_visibility:  # forecast = risk
                    if observation < offshore_limit_visibility:  # observation = risk
                        score = 0
                    else:  # observation = no risk
                        score = 1
                else:  # no risk predicted --> flag '2' for no risk
                    score = 2

                return score

            def offshoreSignificantWaveHeightScore(forecast, observation, location):
                # chose limits based on location
                if 'Nordsee Ost' in location:
                    limit = offshore_limit_significant_wave_height_Nordsee_Ost
                else:
                    limit = offshore_limit_significant_wave_height

                if forecast is None or observation is None:  # no forecast or observation --> invalid score
                    score = 999
                elif forecast > limit:  # forecast = risk
                    if observation > limit:  # observation = risk
                        score = 0
                    else:  # observation = no risk
                        score = 1
                else:  # no risk predicted --> flag '2' for no risk
                    score = 2

                return score

            def offshoreMaximumWaveHeightScore(forecast, observation):
                if observation is None or forecast is None:  # no forecast or observation --> invalid score
                    score = 999
                elif forecast > offshore_limit_maximum_wave_height:  # forecast = risk
                    if observation > offshore_limit_maximum_wave_height:  # observation = risk
                        score = 0
                    else:  # observation = no risk
                        score = 1
                else:  # no risk predicted --> flag '2' for no risk
                    score = 2

                return score

            def onshoreWindSpeedScore(forecast, observation):
                # wind speed has three limits for onshore --> one forecast / observations would count three times
                # if different score for each limit are created
                # --> detect if most 'dangerous' predicted limit was also observed
                # 'lower' limits are necessarily correct if the highest limit was correctly predicted
                # example 1: forecast = 26ms (> 25ms) --> is the observed wind speed > 25 ms, if yes,
                # wind speed > 20 and wind speed > 15 are necessarily correctly predicted
                # if upper limit is wrong but lower limits are correct
                # (e.g. fcst = 16ms, obs = 21ms) risk not correctly predicted but not a false positive
                # --> false positive = limit was predicted but not observed
                if observation is None or forecast is None:  # no forecast or observation --> invalid score
                    windspeedScore = 999
                    return windspeedScore

                if forecast > onshore_limit_windspeed_wind_farm:  # forecast = risk wind farm (> 25ms)
                    if observation > onshore_limit_windspeed_wind_farm:  # observation = risk wind farm
                        windspeedScore = 0
                    else:  # observation = no risk wind farm
                        windspeedScore = 1
                elif forecast > onshore_limit_windspeed_nacelle:  # forecast = risk nacelle (> 20ms)
                    if observation > onshore_limit_windspeed_nacelle:  # observation = risk nacelle
                        windspeedScore = 0
                    else:  # observation = no risk nacelle
                        windspeedScore = 1
                elif forecast > onshore_limit_windspeed_hub:  # forecast = risk hub (> 15ms)
                    if observation > onshore_limit_windspeed_hub:  # observation = risk hub
                        windspeedScore = 0
                    else:  # observation = no risk hub
                        windspeedScore = 1
                else:  # forecast = no risk wind farm --> flag '2' for no risk
                    windspeedScore = 2
                return windspeedScore

            def binaryFalsePositiveScore(forecast, observation):
                if observation is None or forecast is None:  # no forecast or observation --> invalid score
                    score = 999
                elif forecast == 1:  # forecast = risk
                    if observation == 1:  # observation = risk
                        score = 0
                    else:  # observation = no risk
                        score = 1
                else:  # forecast = no risk --> flag '2' for no risk
                    score = 2

                return score

            # calculate the FPS for onshore forecasts (for each relevant parameter in an hourly forecast) --> equation (7.9) in my thesis
            def recordFalsePositiveScoreOnshore(forecastProvider, forecasts, observations):
                if forecastProvider == 'Visual Crossing':
                    table = 'false_positive_score_visual_crossing'
                    constraint = 'unique_timestamp_visual_crossing_false_positive_score'
                elif forecastProvider == 'weather api':
                    table = 'false_positive_score_weather_api'
                    constraint = 'unique_timestamp_weather_api_false_positive_score'
                elif forecastProvider == 'World Weather Online':
                    table = 'false_positive_score_world_weather_online'
                    constraint = 'unique_timestamp_world_weather_online_false_positive_score'
                elif forecastProvider == 'World Weather Online Marine':
                    table = 'false_positive_score_world_weather_online_marine'
                    constraint = 'unique_timestamp_wwo_marine_false_positive_score'
                else:
                    print('Not a valid forecast provider! Valid providers are: "Visual Crossing", "weather api", '
                          '"World Weather Online", "World Weather Online Marine"')
                    return

                for f in forecasts:
                    for o in observations:
                        if f['prediction_date'] == o['observation_date'] and f['prediction_time'] == \
                                o['observation_time']:
                            # relevant parameters onshore: snow, ice, lightning, temperature, wind speed
                            snowScore = binaryFalsePositiveScore(f['snow'], o['snow'])
                            iceScore = binaryFalsePositiveScore(f['ice'], o['ice'])
                            lightningScore = binaryFalsePositiveScore(f['lightning'], o['lightning'])
                            temperatureScore = onshoreTemperatureScore(f['temperature_c'], o['temperature_c'])
                            windspeedScore = onshoreWindSpeedScore(f['windspeed_m_s'], o['windspeed_m_s'])

                            cur.execute('''
                                INSERT INTO {} (prediction_date, prediction_time, location_name, temperature_score,
                                windspeed_score, snow_score, ice_score, lightning_score)
                                VALUES ('{}', '{}', '{}', {}, {}, {}, {}, {})
                                ON CONFLICT ON CONSTRAINT {} DO UPDATE
                                SET temperature_score = {}, windspeed_score = {}, snow_score = {}, ice_score = {}, 
                                lightning_score = {}'''
                                        .format(table, f['prediction_date'], f['prediction_time'], f['location_name'],
                                                temperatureScore, windspeedScore, snowScore, iceScore, lightningScore,
                                                constraint, temperatureScore, windspeedScore, snowScore, iceScore,
                                                lightningScore))

            # calculate the FPS for offshore forecasts (for each relevant parameter in an hourly forecast) --> equation (7.9) in my thesis
            # calculate all offshore parameters and ignore them later if not needed
            def recordFalsePositiveScoreOffshore(forecastProvider, forecasts, observations):
                if forecastProvider == 'Visual Crossing':
                    table = 'false_positive_score_visual_crossing'
                    constraint = 'unique_timestamp_visual_crossing_false_positive_score'
                elif forecastProvider == 'weather api':
                    table = 'false_positive_score_weather_api'
                    constraint = 'unique_timestamp_weather_api_false_positive_score'
                elif forecastProvider == 'World Weather Online':
                    table = 'false_positive_score_world_weather_online'
                    constraint = 'unique_timestamp_world_weather_online_false_positive_score'
                elif forecastProvider == 'World Weather Online Marine':
                    table = 'false_positive_score_world_weather_online_marine'
                    constraint = 'unique_timestamp_wwo_marine_false_positive_score'
                else:
                    print('Not a valid forecast provider! Valid providers are: "Visual Crossing", "weather api", '
                          '"World Weather Online", "World Weather Online Marine"')
                    return

                for f in forecasts:
                    for o in observations:
                        if f['prediction_date'] == o['observation_date'] and f['prediction_time'] == \
                                o['observation_time']:
                            # relevant parameters Offshore (general): snow, ice, lightning, temperature, wind speed,
                            # wind gusts, significant wave height, maximum wave height, visibility
                            snowScore = binaryFalsePositiveScore(f['snow'], o['snow'])
                            iceScore = binaryFalsePositiveScore(f['ice'], o['ice'])
                            lightningScore = binaryFalsePositiveScore(f['lightning'], o['lightning'])
                            temperatureScore = offshoreTemperatureScore(f['temperature_c'], o['temperature_c'])
                            windspeedScore = offshoreWindScore(f['windspeed_m_s'], o['windspeed_m_s'],
                                                               f['location_name'])
                            windgustScore = offshoreWindScore(f['windgusts_m_s'], o['windgusts_m_s'],
                                                              f['location_name'])
                            significantWaveHeightScore = offshoreSignificantWaveHeightScore(
                                f['significant_wave_height_m'], o['significant_wave_height_m'], f['location_name'])
                            maximumWaveHeightScore = offshoreMaximumWaveHeightScore(f['maximum_wave_height_m'],
                                                                                    o['maximum_wave_height_m'])
                            visibilityScore = offshoreVisibilityScore(f['visibility_km'], o['visibility_km'])

                            cur.execute('''
                                INSERT INTO {} (prediction_date, prediction_time, location_name, temperature_score, 
                                windspeed_score, windgust_score, significant_wave_height_score, 
                                maximum_wave_height_score, visibility_score, snow_score, ice_score, lightning_score)
                                VALUES ('{}', '{}', '{}', {}, {}, {}, {}, {}, {}, {}, {}, {})
                                ON CONFLICT ON CONSTRAINT {} DO UPDATE
                                SET temperature_score = {}, windspeed_score = {}, windgust_score = {}, 
                                significant_wave_height_score = {}, maximum_wave_height_score = {}, 
                                visibility_score = {}, snow_score = {}, ice_score = {}, lightning_score = {}
                                '''.format(table, f['prediction_date'], f['prediction_time'], f['location_name'],
                                           temperatureScore, windspeedScore, windgustScore, significantWaveHeightScore,
                                           maximumWaveHeightScore, visibilityScore, snowScore, iceScore,
                                           lightningScore, constraint, temperatureScore, windspeedScore, windgustScore,
                                           significantWaveHeightScore, maximumWaveHeightScore, visibilityScore,
                                           snowScore, iceScore, lightningScore))

            # calculate the FPS for a provider at a location for a specific parameter --> equation (7.10) in my thesis
            def parameterFalsePositiveScore(forecastProvider, location, scores, scoreName):
                totalScoreSum = 0  # variable that stores the current total score sum
                numberOfRecords = 0  # variables that stores the number of considered records (not 999 scores and not 2)
                onlyInvalid = True  # variable that represents whether valid scores exist (observations exist --> 0/1/2)

                for s in scores:
                    if s[scoreName] is None:
                        print('Error: Not all total scores have been calculated!')
                        return
                    elif s[scoreName] == 999:   # score = 999 --> invalid (observation or forecast missing) --> ignore
                        continue
                    elif s[scoreName] == 2:  # score = 2 --> no risk predicted --> ignore
                        onlyInvalid = False
                        continue
                    else:
                        onlyInvalid = False
                        totalScoreSum += s[scoreName]
                        numberOfRecords += 1

                if onlyInvalid == True:  # no observations / forecasts --> no valid scores
                    totalScore = 999
                elif numberOfRecords == 0:  # no risks predicted --> no valid score --> flag '2'
                    totalScore = 2
                else:
                    totalScore = totalScoreSum / numberOfRecords

                cur.execute('''
                    INSERT INTO scores (location_name, forecast_source, score_type, {})
                    VALUES ('{}', '{}', 'False Positive Score', {})
                    ON CONFLICT ON CONSTRAINT unique_source_and_score_type_per_location DO UPDATE
                    SET {} = {}
                    '''.format(scoreName, location, forecastProvider, totalScore, scoreName,
                               totalScore))

            # total false positive score for a provider and location --> equation (7.11) in my thesis
            def totalFalsePositiveScore(forecastProvider, location, scores, relevantParameters):
                # total safety risk detection score =  number of dangers that were incorrectly forecast (False
                # Positive) / number of risks that were forecast
                # risk predicted --> false positive score = 0 or 1 (correctly forecast or not)
                # 1 = incorrect (false positive), 0 = correct (true positive)
                amountOfPredictedRisks = 0
                amountOfFalsePositives = 0

                for s in scores:
                    for p in relevantParameters:
                        if s[p] == 0:
                            amountOfPredictedRisks += 1
                        elif s[p] == 1:
                            amountOfPredictedRisks += 1
                            amountOfFalsePositives += 1

                if amountOfPredictedRisks == 0:  # no risks predicted --> flag '2' for not risks
                    totalScore = 2
                else:
                    totalScore = amountOfFalsePositives / amountOfPredictedRisks

                cur.execute('''
                    INSERT INTO scores (location_name, forecast_source, score_type, total_score)
                    VALUES ('{}', '{}', 'False Positive Score', {})
                    ON CONFLICT ON CONSTRAINT unique_source_and_score_type_per_location DO UPDATE
                    SET total_score = {}
                    '''.format(location, forecastProvider, totalScore, totalScore))

            onshore_locations = ('Putlitz', 'Lindhurst', 'Barzowice')
            offshore_locations = ('Nordsee Ost Merged', 'Gwynt-y-Mor Merged')

            onshoreScores = ('snow_score', 'ice_score', 'lightning_score', 'temperature_score', 'windspeed_score')
            offshoreScores = ('snow_score', 'ice_score', 'lightning_score', 'temperature_score', 'windspeed_score',
                              'windgust_score', 'visibility_score', 'significant_wave_height_score',
                              'maximum_wave_height_score')
            nordseeOstScores = ('snow_score', 'ice_score', 'lightning_score', 'temperature_score', 'windspeed_score',
                                'windgust_score', 'visibility_score', 'significant_wave_height_score')

            forecastProviders = ('Visual Crossing', 'weather api', 'World Weather Online', 'World Weather Online Marine'
                                 )

            # Parameter lists without visibility --> observations seem rounded --> scores might be invalid
            offshoreScoresNoVisibility = ('snow_score', 'ice_score', 'lightning_score', 'temperature_score',
                                          'windspeed_score', 'windgust_score', 'significant_wave_height_score',
                                          'maximum_wave_height_score')
            nordseeOstScoresNoVisibility = ('snow_score', 'ice_score', 'lightning_score', 'temperature_score',
                                            'windspeed_score', 'windgust_score', 'significant_wave_height_score')

            # calculate false positive scores for every record depending on the technology
            for location in onshore_locations:
                for provider in forecastProviders:
                    start_time = time.time()  # start code timer
                    forecasts, observations = selectRecords(provider, location)
                    recordFalsePositiveScoreOnshore(provider, forecasts, observations)
                    time_score = time.time()
                    print(provider, location, 'is done (', time_score - start_time, 'seconds)')  # print status message

            for location in offshore_locations:
                for provider in forecastProviders:
                    start_time = time.time()  # start code timer
                    forecasts, observations = selectRecords(provider, location)
                    recordFalsePositiveScoreOffshore(provider, forecasts, observations)
                    time_score = time.time()
                    print(provider, location, 'is done (', time_score - start_time, 'seconds)')  # print status message
            
            # calculate the total false positive score per parameter for each location and forecast provider
            # Onshore
            for location in onshore_locations:
                for provider in forecastProviders:
                    scores = selectScores(provider, location)
                    for scoreName in onshoreScores:
                        start_time = time.time()  # start code timer
                        parameterFalsePositiveScore(provider, location, scores, scoreName)
                        time_score = time.time()
                        print('Total parameter score for', scoreName, provider, location, 'is done (', time_score -
                              start_time, 'seconds)')  # print status message
            
            # Offshore
            for location in offshore_locations:
                for provider in forecastProviders:
                    scores = selectScores(provider, location)
                    for scoreName in offshoreScores:
                        start_time = time.time()  # start code timer
                        parameterFalsePositiveScore(provider, location, scores, scoreName)
                        time_score = time.time()
                        print('Total parameter score for', scoreName, provider, location, 'is done (',
                              time_score -
                              start_time, 'seconds)')  # print status message
            
            # calculate the total false positive score for each location and forecast provider depending on the
            # technology
            # Onshore
            for location in onshore_locations:
                for provider in forecastProviders:
                    scores = selectScores(provider, location)
                    totalFalsePositiveScore(provider, location, scores, onshoreScores)

            # Offshore
            # Nordsee Ost has custom parameters:
            for provider in forecastProviders:
                scores = selectScores(provider, 'Nordsee Ost Merged')
                totalFalsePositiveScore(provider, 'Nordsee Ost Merged', scores, nordseeOstScores)

            # Gwynt-y-Mor (basic parameters)
            for provider in forecastProviders:
                scores = selectScores(provider, 'Gwynt-y-Mor Merged')
                totalFalsePositiveScore(provider, 'Gwynt-y-Mor Merged', scores, offshoreScores)

            notify()  # notify user when calculations are done

except Exception as error:
    print(error)
finally:
    # close DB connection
    if conn is not None:
        conn.close()
