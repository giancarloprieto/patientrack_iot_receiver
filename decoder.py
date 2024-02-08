from datetime import datetime

import pytz


class Decoder:
    payload = None
    device_identifier = None
    device_datetime = None
    server_datetime = None
    value = None
    variables_data = {}

    def __init__(self, payload):
        self.payload = payload

    def is_valid(self):
        return bool(';' in self.payload and len(self.payload.split(';')) >= 4)

    def decode_payload(self):
        """
        payload example: 764563;2024-01-30 22:55:00;TMP;36.8;HR;120;Sp0;95
        :return:
        """
        data = self.payload.split(';')
        self.device_identifier = data[0]
        self.server_datetime = datetime.utcnow().replace(tzinfo=pytz.UTC)
        self.device_datetime = datetime.strptime(data[1], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)

        for i in range(2, len(data), 2):
            key = data[i]
            value = data[i + 1]
            self.variables_data[key] = float(value)

