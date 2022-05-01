import datetime

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

    def aggregate_data(self, deviceid, category, date, hour_of_day):
        filtered_data = list(self.retrieve_filtered_data(deviceid, category, date, hour_of_day))
        #self.print_filtered_data(filtered_data)
        print(f'Length of filtered data is ', len(filtered_data))
        print(self.get_min_max_avg_data(deviceid, filtered_data))

    def get_min_max_avg_data(self, deviceid, filtered_data):
        min_value = Decimal(0.0)
        max_value = Decimal(0.0)
        avg_value = Decimal(0.0)
        orig_current_min = 0
        counter_for_avg = 0

        dict_min_max_avg_per_min = {}
        list_min_max_avg_per_min = []

        for data in filtered_data:
            # to Check the one minute interval
            current_min = datetime.datetime.strptime(data['timestamp'], "%Y-%m-%d %H:%M:%S.%f").strftime("%M")
            #print(f'Current min is {current_min}')
            if orig_current_min == current_min:
                # to eliminate 0 value in the comparison
                min_value = data['value']
                if data['value'] > max_value:
                    max_value = data['value']
                if data['value'] < min_value:
                    min_value = data['value']
                avg_value = avg_value + data['value']
                counter_for_avg += 1
            else:
                orig_current_min = current_min
                print(avg_value.__float__()/counter_for_avg)
                dict_min_max_avg_per_min = {
                    'device_id': deviceid,
                    'min_val': min_value,
                    'max_val': max_value,
                    'avg_val': avg_value,
                    'timestamp': '00:00',
                }
                counter_for_avg = 0
                list_min_max_avg_per_min.append(dict_min_max_avg_per_min)
                max_value = Decimal(0.0)
                avg_value = Decimal(0.0)

                min_value = data['value']
                if data['value'] > max_value:
                    max_value = data['value']
                if data['value'] < min_value:
                    min_value = data['value']
                avg_value = avg_value + data['value']

        print(len(list_min_max_avg_per_min))
        return min_value, max_value, avg_value/len(filtered_data)
