from Database import Database
from AggregateModel import AggregateModel

database = Database()
aggModel = AggregateModel()

database.setup_tables()

database.retrieve_data('bsm_raw_data', 'BSM_G102', 1, 19)
aggModel.aggregate_data('BSM_G102', 'HeartRate', 1, 19)
aggModel.aggregate_data('BSM_G102', 'Temperature', 1, 19)
aggModel.aggregate_data('BSM_G101', 'SPO2', 1, 19)


