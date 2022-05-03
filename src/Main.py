from Database import Database
from AggregateModel import AggregateModel
from AlertDataModel import AlertDataModel
from RawDataModel import RawDataModel

database = Database()
rawDataModel = RawDataModel()
aggModel = AggregateModel()
alertModel = AlertDataModel()

start_date = 2
start_hour = 15
print("##### Starting the BSM Hospital Monitoring system #####")

database.setup_tables()

print("\n\n ##### Retrieving data for device BSM_G101 ##### ")
rawDataModel.get_data_by_device_id("BSM_G101")

print("\n\n ##### Inserting aggregated data for device BSM_G101 ##### ")
aggModel.aggregate_data('BSM_G101', 'SPO2', start_date, start_hour)
aggModel.aggregate_data('BSM_G101', 'HeartRate', start_date, start_hour)
aggModel.aggregate_data('BSM_G101', 'Temperature', start_date, start_hour)

print("\n\n ##### Inserting aggregated data for device BSM_G102 ##### ")
aggModel.aggregate_data('BSM_G102', 'SPO2', start_date, start_hour)
aggModel.aggregate_data('BSM_G102', 'HeartRate', start_date, start_hour)
aggModel.aggregate_data('BSM_G102', 'Temperature', start_date, start_hour)

#print(aggModel.retrieve_aggregated_data_by_device("BSM_G101"))

#print(aggModel.retrieve_aggregated_data_by_device_and_category("BSM_G101", "SPO2"))

print("\n\n #### Following System Thresholds are defined for this application, for making changes please refer (BSMRules.py) ####")
alertModel.print_system_thresholds()

print("\n\n ##### Checking anomalies for BSM_G101 #####")
alertModel.check_anomaly_in_data_by_category("BSM_G101", "HeartRate")
alertModel.check_anomaly_in_data_by_category("BSM_G101", "Temperature")
alertModel.check_anomaly_in_data_by_category("BSM_G101", "SPO2")

print("\n\n ##### Checking anomalies for BSM_G102 #####")
alertModel.check_anomaly_in_data_by_category("BSM_G102", "HeartRate")
alertModel.check_anomaly_in_data_by_category("BSM_G102", "Temperature")
alertModel.check_anomaly_in_data_by_category("BSM_G102", "SPO2")
