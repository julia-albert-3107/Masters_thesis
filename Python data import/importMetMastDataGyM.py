import os
import psycopg2
import psycopg2.extras
from datetime import datetime
import pytz
from datetime import timedelta
import csv
import pandas as pd

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
        host = hostname,
        dbname = database,
        user = username,
        password = pwd,
        port = port_id
    ) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # wind farm 'Meta data'
            location_name = 'Gwynt-y-Mor Met Mast'
            technology = 'Offshore'
            latitude = 53.480733
            longitude = -3.5084
            timezone_local = 'Europe/London'
            dst_double_hour_local = '2022-10-30 01:00:00+00:00'

            query = '''
                INSERT INTO weather_data (observation_date, observation_time, location_name, technology, longitude, 
                latitude, temperature_c, windspeed_m_s, windgusts_m_s, original_source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'met mast')
                ON CONFLICT ON CONSTRAINT unique_timestamp_and_location DO UPDATE
                SET temperature_c = %s, windspeed_m_s = %s, windgusts_m_s = %s, original_source = 'met mast'
                '''

            def convertWindSpeed(original_height, conversion_height, original_windspeed):
                if original_windspeed is None:
                    return None
                else:
                    windspeed = original_windspeed * pow((conversion_height / original_height), (1 / 7))
                    return windspeed


            with open('GyM/Gwynt-y-Mor Met Data.csv') as input_file: 
                file = csv.reader(input_file, delimiter=',')

                # file with data from 2022 & 2023 in 10 minute intervals --> 6 per hour
                # only data from 2022 is relevant --> 3D array --> array[month][day][hour]
                # Not every month has the same amount of days --> initialize with 'None' and only update database if
                # value is not None
                # 1. dimension: month (1 - 12) --> index = month - 1
                # 2. dimension: day (1 - 31) --> index = day - 1
                # 3. dimension: hour (0 - 23) --> index = hour
                # example 1: 01.01. 00:00 --> array[0][0][0]
                # example 2: 31.07. 13:00 --> array[6][30][13]

                # arrays for parameters
                windspeed_year = [[[None for k in range(24)] for j in range(31)] for i in range(12)]
                windgusts_year = [[[None for k in range(24)] for j in range(31)] for i in range(12)]
                temperature_year = [[[None for k in range(24)] for j in range(31)] for i in range(12)]
                # counter that counts the existing values per hour --> calculate an average
                # sometimes measurements are missing --> e.g. 5 measurements per hour --> average = sum / 5 (not 6)
                counter_temperature_records_per_hour = \
                    [[[None for k in range(24)] for j in range(31)] for i in range(12)]

                next(file)  # ignore the header
                for line in file:
                    time_stamp_utc = line[0]
                    date_utc = time_stamp_utc[0:10]
                    year = date_utc[6:]
                    time_utc = time_stamp_utc[11:]

                    if year != '2022':  # only consider 2022
                        continue

                    if date_utc[3] == "0":
                        month_utc = date_utc[4:5]
                    else:
                        month_utc = date_utc[3:5]

                    if date_utc[0] == "0":
                        day_utc = date_utc[1:2]
                    else:
                        day_utc = date_utc[0:2]

                    if time_utc[0] == "0":
                        hour_utc = time_utc[1:2]
                    else:
                        hour_utc = time_utc[0:2]

                    if time_utc[3] == "0":
                        minute_utc = time_utc[4:5]
                    else:
                        minute_utc = time_utc[3:5]

                    windspeed = line[1]
                    windgusts = line[2]
                    temperature = line[3]

                    # wind speed --> use maximum value per hour
                    # (most dangerous value and consistent with weather station)
                    if not (windspeed == ''):
                        # current maximum is None --> current wind speed = max
                        if windspeed_year[int(month_utc) - 1][int(day_utc) - 1][int(hour_utc)] is None:
                            windspeed_year[int(month_utc) - 1][int(day_utc) - 1][int(hour_utc)] = float(windspeed)
                        # current maximum is smaller than current wind speed --> current wind speed = max
                        elif windspeed_year[int(month_utc) - 1][int(day_utc) - 1][int(hour_utc)] < float(windspeed):
                            windspeed_year[int(month_utc) - 1][int(day_utc) - 1][int(hour_utc)] = float(windspeed)

                    # wind gusts --> use maximum value per hour
                    # most dangerous value and consistent with weather station
                    if not (windgusts == ''):
                        # current maximum is None --> current wind gusts = max
                        if windgusts_year[int(month_utc) - 1][int(day_utc) - 1][int(hour_utc)] is None:
                            windgusts_year[int(month_utc) - 1][int(day_utc) - 1][int(hour_utc)] = float(windgusts)
                        # current maximum is smaller than current wind gusts --> current wind gusts = max
                        elif windgusts_year[int(month_utc) - 1][int(day_utc) - 1][int(hour_utc)] < float(windgusts):
                            windgusts_year[int(month_utc) - 1][int(day_utc) - 1][int(hour_utc)] = float(windgusts)

                    # temperature --> use average temperature per hour
                    # both high and low temperatures are dangerous and consistent with weather station
                    if not (temperature == ''):
                        # increment counter
                        if counter_temperature_records_per_hour[int(month_utc) - 1][int(day_utc) - 1][int(hour_utc)] \
                                is None:
                            counter_temperature_records_per_hour[int(month_utc) - 1][int(day_utc) - 1][int(hour_utc)] \
                                = 1
                        else:
                            counter_temperature_records_per_hour[int(month_utc) - 1][int(day_utc) - 1][int(hour_utc)] \
                                += 1

                        # add temperature to current sum
                        if temperature_year[int(month_utc) - 1][int(day_utc) - 1][int(hour_utc)] is None:
                            temperature_year[int(month_utc) - 1][int(day_utc) - 1][int(hour_utc)] = float(temperature)
                        else:
                            temperature_year[int(month_utc) - 1][int(day_utc) - 1][int(hour_utc)] += float(temperature)

                # insert values into database
                for month in range(1, 13):  # 1 - 12
                    for day in range(1, 32):  # 1 - 31
                        for hour in range(24):  # 0 - 23
                            windspeed_hour = windspeed_year[month - 1][day - 1][hour]
                            windgusts_hour = windgusts_year[month - 1][day - 1][hour]

                            # wind speed and wind gusts measured at 30m
                            # --> convert to usual wind measurements height (10m)
                            windspeed_hour = convertWindSpeed(30, 10, windspeed_hour)
                            windgusts_hour = convertWindSpeed(30, 10, windgusts_hour)

                            # true if no records were measured for this hour, or it doesn't exist (e.g. 31.02.)
                            if windspeed_hour is None and windgusts_hour is None and \
                                    counter_temperature_records_per_hour[month - 1][day - 1][hour] is None:
                                continue

                            # if at least one record for temperature --> calculate average
                            if counter_temperature_records_per_hour[month - 1][day - 1][hour] is not None:
                                temperature_hour = temperature_year[month - 1][day - 1][hour] / \
                                                   counter_temperature_records_per_hour[month - 1][day - 1][hour]

                            # create time stamps from loop iterators
                            # time
                            if hour < 10:  # add 0 before hour --> PostgreSQL format for time
                                hour_utc = '0' + str(hour)
                            else:
                                hour_utc = str(hour)
                            observation_time = hour_utc + ':00:00'

                            # date
                            if day < 10:  # add 0 before day
                                day_utc = '0' + str(day)
                            else:
                                day_utc = str(day)

                            if month < 10:  # add 0 before month
                                month_utc = '0' + str(month)
                            else:
                                month_utc = str(month)
                            observation_date = '2022-' + month_utc + '-' + day_utc

                            # convert UTC to local time
                            dateTime_utc = observation_date + ' ' + observation_time  # merge date and time string
                            dateTime_utc = datetime.strptime(dateTime_utc, '%Y-%m-%d %H:%M:%S')  # convert to datetime
                            dateTime_utc = pytz.utc.localize(dateTime_utc)  # add time zone (UTC)
                            dateTime_local = dateTime_utc.astimezone(pytz.timezone(timezone_local))

                            # DST: 2022-10-30 01:00:00 happens twice --> adjust second time stamp by 1 second to avoid
                            # unique constraint
                            dst_double_hour = datetime.strptime(dst_double_hour_local, '%Y-%m-%d %H:%M:%S%z')
                            if dateTime_local == dst_double_hour:
                                dateTime_local = dateTime_local + timedelta(seconds=1)

                            # insert data into database
                            data = (dateTime_local.date(), dateTime_local.time(), location_name, technology,
                                    longitude, latitude, temperature_hour, windspeed_hour, windgusts_hour,
                                    temperature_hour, windspeed_hour, windgusts_hour)
                            cur.execute(query, data)

except Exception as error:
    print(error)
finally:
    # close DB connection
    if conn is not None:
        conn.close()
