from json_logic import jsonLogic
from decimal import Decimal


class BSMRules:

    heart_rate_min = 70
    heart_rate_max = 115

    temp_min = 96
    temp_max = 99

    spo2_min = 92
    spo2_max = 99

    heart_rate_rule = {"or": [
        {"<": [{"var": "min_val"}, Decimal(heart_rate_min)]},
        {">": [{"var": "max_val"}, Decimal(heart_rate_max)]}
    ]}

    temperature_rule = {"or": [
        {"<": [{"var": "min_val"}, Decimal(temp_min)]},
        {">": [{"var": "max_val"}, Decimal(temp_max)]}
    ]}

    spo2_rule = {"or": [
        {"<": [{"var": "min_val"}, Decimal(spo2_min)]},
        {">": [{"var": "max_val"}, Decimal(spo2_max)]}
    ]}

    def print_rules(self):
        print(f'For Heart rate min and max values are defined as '
              f'{BSMRules.heart_rate_min} and {BSMRules.heart_rate_max} respectively')
        print(f'For Temperature min and max values are defined as '
              f'{BSMRules.temp_min} and {BSMRules.temp_max} respectively')
        print(f'For SPO2 min and max values are defined as '
              f'{BSMRules.spo2_min} and {BSMRules.spo2_max} respectively')
