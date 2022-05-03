import datetime
from random import random

from Database import Database
from decimal import Decimal


class AggregateModel:

    def __init__(self):
        self.database = Database()

    def retrieve_filtered_data(self, deviceid, category, date, hour_of_day):
        items = self.database.retrieve_data("bsm_raw_data", deviceid, date, hour_of_day)

        if category == "HeartRate":
            filtered_data = filter(self.__filter_heart_rate, items)
        elif category == "Temperature":
            filtered_data = filter(self.__filter_temp, items)
        elif category == "SPO2":
            filtered_data = filter(self.__filter_spo2, items)

        return filtered_data

    def print_filtered_data(self, filtered_data):
        for data in filtered_data:
            print(data)

    def __filter_heart_rate(self, data):
        if data["datatype"] == 'HeartRate':
            return True
        else:
            return False

    def __filter_temp(self, data):
        if data["datatype"] == 'Temperature':
            return True
        else:
            return False

    def __filter_spo2(self, data):
        if data["datatype"] == 'SPO2':
            return True
        else:
            return False

    # This function will perform two functions, first it will get filtered data based on date and hour.
    # And second it fetches min, max and avg values per min and then inserts them into DB
    def aggregate_data(self, deviceid, category, date, hour_of_day):
        filtered_data = list(self.retrieve_filtered_data(deviceid, category, date, hour_of_day))
        print(f'Length of filtered data is ', len(filtered_data))

        list_of_min_max_avg_data_per_min = self.get_min_max_avg_data(deviceid, category, filtered_data)
        self.database.insert_data('bsm_agg_data', list_of_min_max_avg_data_per_min)
        print(
            f'Total {len(list_of_min_max_avg_data_per_min)} per minute records for {category} and device {deviceid} inserted successfully')

    # This function will get the min, max and average values based on deviceid and category
    # it also accepts the filtered data which we have retrieved as part of aggregate_data function
    def get_min_max_avg_data(self, deviceid, category, filtered_data):
        min_value = Decimal(0.0)
        max_value = Decimal(0.0)
        avg_value = Decimal(0.0)
        orig_current_min = 0
        counter_for_avg = 0

        dict_min_max_avg_per_min = {}
        list_min_max_avg_per_min = []

        for data in filtered_data:
            if counter_for_avg == 0:
                # to eliminate 0 value in the comparison
                min_value = data['value']
            # to Check the one minute interval
            current_min = int(datetime.datetime.strptime(data['timestamp'], "%Y-%m-%d %H:%M:%S.%f").strftime("%M"))
            # print(f'Current min is {current_min} and orig current min is {orig_current_min}')
            if orig_current_min == current_min:
                if data['value'] > max_value:
                    max_value = data['value']
                if data['value'] < min_value:
                    min_value = data['value']
                avg_value = avg_value + data['value']
                counter_for_avg = counter_for_avg + 1
            else:
                # print(counter_for_avg)
                # print(str(datetime.datetime.strptime(data['timestamp'], "%Y-%m-%d %H:%M:%S.%f")
                #          .replace(minute=int(orig_current_min)).strftime('%Y-%m-%d %H:%M:%S')))
                if counter_for_avg > 0:
                    dict_min_max_avg_per_min = {
                        'deviceid': deviceid,
                        'datatype': category,
                        'min_val': min_value,
                        'max_val': max_value,
                        'avg_val': "{:.2f}".format(float(avg_value) / counter_for_avg),
                        'timestamp': str(datetime.datetime.strptime(data['timestamp'], "%Y-%m-%d %H:%M:%S.%f")
                                         .replace(minute=int(orig_current_min)).strftime('%Y-%m-%d %H:%M:%S')),
                    }
                    list_min_max_avg_per_min.append(dict_min_max_avg_per_min)

                # Resetting all values for next minute calculation
                orig_current_min = current_min
                counter_for_avg = 1
                max_value = Decimal(0.0)

                min_value = data['value']
                if data['value'] > max_value:
                    max_value = data['value']
                if data['value'] < min_value:
                    min_value = data['value']
                avg_value = data['value']

        return list_min_max_avg_per_min

    def retrieve_aggregated_data_by_device(self, deviceid):
        return self.database.retrieve_data('bsm_agg_data', deviceid)

    def retrieve_aggregated_data_by_device_and_category(self, deviceid, category):
        return self.database.retrieve_data('bsm_agg_data', deviceid, None, None, category)
