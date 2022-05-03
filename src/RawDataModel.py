from Database import Database

class RawDataModel:

    def __init__(self):
        self._database = Database()

    def get_data_by_device_id(self, deviceid):
        items = self._database.retrieve_data("bsm_raw_data", deviceid)
        print(f'Total {len(items)} records retrieved for device {deviceid}')

    def get_data_by_device_id_and_category(self, deviceid, category):
        items = self._database.retrieve_data("bsm_agg_data", deviceid, None, None, category)
        print(f'Total {len(items)} records retrieved for device {deviceid} and category {category}')
