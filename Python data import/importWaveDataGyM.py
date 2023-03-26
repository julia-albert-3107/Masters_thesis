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
            location_name = 'Gwynt-y-Mor Wave Radar'
            technology = 'Offshore'
            latitude = 53.4848123340119
            longitude = -3.61657496302255
            timezone_local = 'Europe/London'
            dst_double_hour_local = '2022-10-30 01:00:00+00:00'

            query = '''
                INSERT INTO weather_data (
                observation_date, observation_time, location_name, technology, longitude, latitude, 
                significant_wave_height_m, maximum_wave_height_m, original_source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'wave radar')
                ON CONFLICT ON CONSTRAINT unique_timestamp_and_location DO UPDATE
                SET significant_wave_height_m = %s, maximum_wave_height_m = %s, original_source = 'wave radar'
                '''

            # This part only has to be executed once to clean up the original files
            """
            original_file_list = os.listdir('GyM/wave_radar/original')

            # remove blank lines between records in csv file with pandas
            for f in original_file_list:
                original_file = pd.read_csv("GyM/wave_radar/original/" + f, skipinitialspace=True)
                original_file.to_csv('GyM/wave_radar/clean/' + f, index=False)
            """

            cleaned_file_list = os.listdir('GyM/wave_radar/clean')

            for f in cleaned_file_list:
                with open("GyM/wave_radar/clean/" + f) as input_file:
                    file = csv.reader(input_file, delimiter=',')

                    # monthly files with records for every minute --> 2D arrays for days x hours
                    # Not every month has the same amount of days --> initialize with 'None' and only update database if
                    # value is not None
                    # 2D arrays with row = days (31), col = hour (24)
                    # Note: hours: 0 - 23 --> index = hour, but days: 1 - 28/30/31 --> index = day - 1
                    # example 1: 01.0X., 00:00 --> array[0][0]
                    # example 2: 27.0X., 15:00 --> array[26][15]

                    maximum_wave_height_month = [[None] * 24 for i in range(31)]
                    significant_wave_height_month = [[None] * 24 for i in range(31)]
                    counter_significant_wave_height_records_per_hour = [[None] * 24 for i in range(31)]

                    for line in file:
                        time_stamp_utc = line[0]
                        time_stamp_utc = time_stamp_utc[3:]  # every time stamp starts with !W --> remove this
                        date_utc = time_stamp_utc[0:10]
                        time_utc = time_stamp_utc[11:]
                        year = date_utc[6:]
                        month_utc = date_utc[3:5]

                        if year != '2022':  # only consider 2022
                            continue

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

                        significant_wave_height = line[3]
                        maximum_wave_height = line[6]

                        # maximum wave height --> use maximum value per hour
                        # ignore missing values
                        if not (maximum_wave_height == ''):
                            if maximum_wave_height_month[int(day_utc) - 1][int(hour_utc)] is None:
                                maximum_wave_height_month[int(day_utc) - 1][int(hour_utc)] = float(maximum_wave_height)
                            elif maximum_wave_height_month[int(day_utc) - 1][int(hour_utc)] < \
                                    float(maximum_wave_height):
                                maximum_wave_height_month[int(day_utc) - 1][int(hour_utc)] = float(maximum_wave_height)

                        # significant wave height = "average of the highest one-third (33%) of waves
                        # (measured from trough to crest) that occur in a given period" --> use the average of all
                        # values per hour
                        if not (significant_wave_height == ''):
                            if counter_significant_wave_height_records_per_hour[int(day_utc) - 1][int(hour_utc)] is \
                                    None:
                                counter_significant_wave_height_records_per_hour[int(day_utc) - 1][int(hour_utc)] = 1
                            else:
                                counter_significant_wave_height_records_per_hour[int(day_utc) - 1][int(hour_utc)] += 1

                            if significant_wave_height_month[int(day_utc) - 1][int(hour_utc)] is None:
                                significant_wave_height_month[int(day_utc) - 1][int(hour_utc)] = \
                                    float(significant_wave_height)
                            else:
                                significant_wave_height_month[int(day_utc) - 1][int(hour_utc)] += \
                                    float(significant_wave_height)

                    for day in range(1, 32):  # 1 - 31
                        for hour in range(24):  # 0 - 23
                            maximum_wave_height_hour = maximum_wave_height_month[day - 1][hour]

                            # true if no records were measured for this hour, or it doesn't exist (e.g. 31.02.)
                            if maximum_wave_height_hour is None and counter_significant_wave_height_records_per_hour[
                                day - 1][hour] is None:
                                continue

                            # if at least one record for significant wave height --> calculate average
                            if counter_significant_wave_height_records_per_hour[day - 1][hour] is not None:
                                significant_wave_height_hour = significant_wave_height_month[day - 1][hour] / \
                                    counter_significant_wave_height_records_per_hour[day - 1][hour]

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
                            observation_date = '2022-' + month_utc + '-' + day_utc

                            # convert UTC to local time
                            dateTimeUTC = observation_date + ' ' + observation_time
                            dateTimeUTC = datetime.strptime(dateTimeUTC, '%Y-%m-%d %H:%M:%S')
                            dateTimeUTC = pytz.utc.localize(dateTimeUTC)
                            dateTime_local = dateTimeUTC.astimezone(pytz.timezone(timezone_local))

                            # DST: 2022-10-30 01:00:00 happens twice --> adjust second time stamp by 1 second to avoid
                            # unique constraint
                            dst_double_hour = datetime.strptime(dst_double_hour_local, '%Y-%m-%d %H:%M:%S%z')
                            if dateTime_local == dst_double_hour:
                                dateTime_local = dateTime_local + timedelta(seconds=1)

                            # insert data into database
                            data = (dateTime_local.date(), dateTime_local.time(), location_name, technology, longitude,
                                    latitude, significant_wave_height_hour, maximum_wave_height_hour,
                                    significant_wave_height_hour, maximum_wave_height_hour)
                            cur.execute(query, data)

except Exception as error:
    print(error)
finally:
    # close DB connection
    if conn is not None:
        conn.close()
