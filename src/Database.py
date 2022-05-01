import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime

class Database:

    def __init__(self):
        # Get the service resource.
        self._dynamodb = boto3.resource('dynamodb')
        self._table_names = [table.name for table in self._dynamodb.tables.all()]

    def __create_data_table(self, table_name, key_schema, attribute_definition):
        if table_name not in self._table_names:
            table = self._dynamodb.create_table(
                TableName=table_name,
                KeySchema=key_schema,
                AttributeDefinitions=attribute_definition,
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            # Wait until the table exists.
            table.wait_until_exists()

            # Print out some data about the table.
            print(f' {table_name} created successfully with record count as {table.item_count}')
        else:
            table = self._dynamodb.Table(table_name)
            print(f'{table_name} already exist with record count as {table.item_count}')

    def create_raw_data_table(self):
        key_schema = [
                        {
                            'AttributeName': 'deviceid',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'timestamp',
                            'KeyType': 'RANGE'
                        }
                    ]
        attribute_definition = [
                                   {
                                       'AttributeName': 'deviceid',
                                       'AttributeType': 'S'
                                   },
                                   {
                                       'AttributeName': 'timestamp',
                                       'AttributeType': 'S'
                                   },
                               ]
        self.__create_data_table('bsm_raw_data', key_schema, attribute_definition)

    def create_aggregate_data_table(self):
        key_schema = [
                        {
                            'AttributeName': 'deviceid',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'timestamp',
                            'KeyType': 'RANGE'
                        }
                    ]
        attribute_definition = [
                                   {
                                       'AttributeName': 'deviceid',
                                       'AttributeType': 'S'
                                   },
                                   {
                                       'AttributeName': 'timestamp',
                                       'AttributeType': 'S'
                                   },
                               ]
        self.__create_data_table('bsm_agg_data', key_schema, attribute_definition)

    def create_anomaly_data_table(self):
        key_schema = [
                        {
                            'AttributeName': 'deviceid',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'timestamp',
                            'KeyType': 'RANGE'
                        }
                    ]
        attribute_definition = [
                                   {
                                       'AttributeName': 'deviceid',
                                       'AttributeType': 'S'
                                   },
                                   {
                                        'AttributeName': 'timestamp',
                                        'AttributeType': 'S'
                                   }
                               ]
        self.__create_data_table('bsm_alert_data', key_schema, attribute_definition)

    def setup_tables(self):
        self.create_raw_data_table()
        self.create_aggregate_data_table()
        self.create_anomaly_data_table()
        print("Table setup completed successfully")

    def retrieve_data(self, table_name, deviceid, date, hour_of_day):

        #This will default to all the date attributes except date and hour_of_day
        start_timestamp = datetime(2022, 5, date, hour_of_day, 0, 0, 1).__str__()
        end_timestamp = datetime(2022, 5, date, hour_of_day+1, 0, 0, 1).__str__()
        table = self._dynamodb.Table(table_name)
        response = table.query(
            KeyConditionExpression=(Key('deviceid').eq(deviceid)
                                   & Key('timestamp').between(start_timestamp, end_timestamp)),
            ScanIndexForward=True
        )
        items = response['Items']
        return items
