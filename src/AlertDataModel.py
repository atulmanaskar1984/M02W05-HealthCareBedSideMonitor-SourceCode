from json_logic import jsonLogic

from Database import Database
from bsm_rules import BSMRules


class AlertDataModel:
    def __init__(self):
        self._bsm_rules = BSMRules()
        self._database = Database()

    def check_anomaly_in_data_by_category(self, deviceid, category):
        items = self._database.retrieve_data("bsm_agg_data", deviceid, None, None, category)
        counter = 0

        if category == "HeartRate":
            rule_details = self._bsm_rules.heart_rate_rule
        elif category == "Temperature":
            rule_details = self._bsm_rules.temperature_rule
        elif category == "SPO2":
            rule_details = self._bsm_rules.spo2_rule

        for item in items:
            if jsonLogic(rule_details, item):
                counter = counter + 1
                # print(counter, item)
                if counter >= 5:
                    print(
                        f'For bed {deviceid} {category} is out of range at the end of {item["timestamp"]}')
                    item['alert_message'] = f'For bed {deviceid} {category} is out of range at the end of {item["timestamp"]}'
                    self.send_data_to_db(item)
                    # At this point alert is sent out so before sending another alert we need to wait
                    # for another 5 occurrences of the similar condition
                    counter = 0
            else:
                counter = 0

    def send_data_to_db(self, item):
        self._database.insert_data('bsm_alert_data', [item])


    def print_system_thresholds(self):
        self._bsm_rules.print_rules()
