import os
import psycopg2
import psycopg2.extras
from datetime import datetime
import pytz
from datetime import timedelta

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
            location_name = 'Nordsee Ost Wave Radar'
            technology = 'Offshore'
            latitude = 54.441667
            longitude = 7.678889
            timezone_local = 'Europe/Berlin'
            dst_double_hour_local = '2022-10-30 02:00:00+0100'

            query = '''
                INSERT INTO weather_data (
                observation_date, observation_time, location_name, technology, longitude, latitude, 
                significant_wave_height_m, maximum_wave_height_m, original_source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'wave radar')
                ON CONFLICT ON CONSTRAINT unique_timestamp_and_location DO UPDATE
                SET significant_wave_height_m = %s, maximum_wave_height_m = %s, original_source = 'wave radar'
                '''

            file_list = os.listdir('NSO')

            for f in file_list:
                with open("NSO/" + f) as file:
                    maximum_wave_height_day = [None for i in range(24)]
                    significant_wave_height_day = [None for i in range(24)]
                    counter_significant_wave_height_records_per_hour = [None for i in range(24)]

                    for line in file:
                        time_stamp_utc = line[0:12]
                        date_utc = line[0:4] + '-' + line[4:6] + '-' + line[6:8]
                        time_utc = line[8:10] + ':' + line[10:12] + ':00'

                        if time_stamp_utc[8] == "0":
                            hour_utc = time_stamp_utc[9:10]
                        else:
                            hour_utc = time_stamp_utc[8:10]

                        if time_stamp_utc[10] == "0":
                            minute_utc = time_stamp_utc[11:12]
                        else:
                            minute_utc = time_stamp_utc[10:12]

                        significant_wave_height = line[17:22]
                        maximum_wave_height = line[62:67]

                        # maximum wave height --> use maximum value per hour
                        # check for all characters in "NULL" separately because NULL values in the source file start one
                        # character earlier than not NULL values (e.g.: maximum_wave_height = 'ULL ')
                        if not (maximum_wave_height.__contains__('N') or maximum_wave_height.__contains__('U')
                                or maximum_wave_height.__contains__('L')):
                            # null value --> ignore, not null compare to current maximum
                            if maximum_wave_height_day[int(hour_utc)] is None:
                                maximum_wave_height_day[int(hour_utc)] = float(maximum_wave_height)
                            elif maximum_wave_height_day[int(hour_utc)] < float(maximum_wave_height):
                                maximum_wave_height_day[int(hour_utc)] = float(maximum_wave_height)

                        # significant wave height = "average of the highest one-third (33%) of waves
                        # (measured from trough to crest) that occur in a given period" --> use the average of all
                        # values per hour
                        if not (significant_wave_height.__contains__('N') or significant_wave_height.__contains__('U')
                                or significant_wave_height.__contains__('L')):  # null value --> ignore
                            if counter_significant_wave_height_records_per_hour[int(hour_utc)] is None:
                                counter_significant_wave_height_records_per_hour[int(hour_utc)] = 1
                            else:
                                counter_significant_wave_height_records_per_hour[int(hour_utc)] += 1

                            if significant_wave_height_day[int(hour_utc)] is None:
                                significant_wave_height_day[int(hour_utc)] = float(significant_wave_height)
                            else:
                                significant_wave_height_day[int(hour_utc)] += float(significant_wave_height)

                    # insert values into database
                    for hour in range(24):  # 0 - 23
                        maximum_wave_height_hour = maximum_wave_height_day[hour]

                        #  no records for this hour exist
                        if maximum_wave_height_hour is None and \
                                counter_significant_wave_height_records_per_hour[hour] is None:
                            continue

                        # if at least one record for significant wave height --> calculate average
                        if counter_significant_wave_height_records_per_hour[hour] is not None:
                            significant_wave_height_hour = significant_wave_height_day[hour] / \
                                                           counter_significant_wave_height_records_per_hour[hour]

                        # create time stamps from loop iterators
                        # time
                        if hour < 10:  # add 0 before hour --> PostgreSQL format for time
                            hour_utc = '0' + str(hour)
                        else:
                            hour_utc = str(hour)
                        observation_time = hour_utc + ':00:00'

                        # convert UTC to local time
                        dateTimeUTC = date_utc + ' ' + observation_time
                        dateTimeUTC = datetime.strptime(dateTimeUTC, '%Y-%m-%d %H:%M:%S')
                        dateTimeUTC = pytz.utc.localize(dateTimeUTC)
                        dateTime_local = dateTimeUTC.astimezone(pytz.timezone(timezone_local))

                        # DST: 2022-10-30 02:00:00 happens twice --> adjust second time stamp by 1 second to avoid
                        # unique constraint
                        dst_double_hour = datetime.strptime(dst_double_hour_local, '%Y-%m-%d %H:%M:%S%z')
                        if dateTime_local == dst_double_hour:
                            dateTime_local = dateTime_local + timedelta(seconds=1)

                        data = (dateTime_local.date(), dateTime_local.time(), location_name, technology, longitude,
                                latitude, significant_wave_height_hour, maximum_wave_height_hour,
                                significant_wave_height_hour, maximum_wave_height_hour)
                        cur.execute(query, data)

                print('{} done'.format(date_utc))

except Exception as error:
    print(error)
finally:
    # close DB connection
    if conn is not None:
        conn.close()
