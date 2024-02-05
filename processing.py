from data_types import Device, Variable, AlarmSettings, Record, Patient
from decoder import Decoder


class Processing:
    payload = None
    db_connector = None
    record = None
    is_alarm = False

    def __init__(self, payload, db_connector):
        self.payload = payload
        self.db_connector = db_connector

    def run(self):
        decoder = Decoder(self.payload)
        if decoder.is_valid():
            decoder.decode_payload()
            self.record = Record()
            self.record.device_identifier = decoder.device_identifier
            self.record.datetime_device = decoder.device_datetime
            self.record.datetime_server = decoder.server_datetime
            self.record.payload = decoder.payload
            self.record.value = decoder.value
            device = self.get_device(decoder.device_identifier)
            variable = self.get_variable(decoder.variable_identifier)
            if device and variable:
                self.record.device_id = device.id
                self.record.variable_id = variable.id
                self.record.variable_name = variable.name
                patient = self.get_patient(device.patient_id)
                if patient:
                    self.record.patient_id = patient.id
                    self.record.patient_identification = patient.identification
                    alarm_settings = self.get_alarm_settings(patient.id, variable.id)
                    self.is_alarm = self.get_is_alarm(decoder.value, alarm_settings)
                    if self.is_alarm:
                        self.record.alarm_settings_fk_id = alarm_settings.id
                        self.record.alarm_name = alarm_settings.name
                        self.record.alarm_operator = alarm_settings.operator
                        self.record.alarm_ref_value = alarm_settings.reference_value
                    self.save()

                else:
                    print(f'NO PATIENT is assigned to device; payload: {self.payload}')
            else:
                print(f'VARIABLE or DEVICE non existent; payload: {self.payload}')
        else:
            print(f'PAYLOAD NOT VALID; payload: {self.payload}')

    def get_device(self, identifier):
        query = f"SELECT id, patient_id FROM device_device WHERE identifier = '{identifier}';"
        result = self.db_connector.fetch_one(query)
        if result:
            return Device._make(result)

    def get_variable(self, identifier):
        query = f"SELECT id, name, identifier FROM device_variable WHERE identifier = '{identifier}';"
        result = self.db_connector.fetch_one(query)
        if result:
            return Variable._make(result)

    def get_patient(self, patient_id):
        query = f"SELECT id, identification FROM patient_patient WHERE id = '{patient_id}';"
        result = self.db_connector.fetch_one(query)
        if result:
            return Patient._make(self.db_connector.fetch_one(query))

    def get_alarm_settings(self, patient_id, variable_id):
        query = f"""
                    SELECT alarmsettings.id, alarmsettings.name,
                     alarmsettings.reference_value, alarmsettings.operator
                FROM
                    monitoring_alarmsettings AS alarmsettings
                INNER JOIN
                    patient_patient_alarm_settings AS patient_alarm_settings
                    ON alarmsettings.id = patient_alarm_settings.alarmsettings_id
                WHERE
                    patient_alarm_settings.patient_id = {patient_id}
                    AND alarmsettings.variable_id = {variable_id};"""
        result = self.db_connector.fetch_one(query)
        if result:
            return AlarmSettings._make(result)

    def get_is_alarm(self, value, alarm_settings):
        if alarm_settings:
            operator = alarm_settings.operator
            ref_value = alarm_settings.reference_value
            if operator == 'E':
                return value == ref_value
            elif operator == 'GT':
                return value > ref_value
            elif operator == 'GTE':
                return value >= ref_value
            elif operator == 'LT':
                return value < ref_value
            else:  # LTE
                return value <= ref_value
        else:
            return False

    def save(self):
        query = f"""
            INSERT INTO monitoring_record (
                created_at,
                last_updated_at,
                datetime_server,
                datetime_device,
                patient_id,
                patient_identification,
                device_id,
                device_identifier,
                variable_id,
                variable_name,
                value,
                payload,
                alarm_settings_fk_id,
                alarm_name,
                alarm_operator,
                alarm_ref_value
            ) VALUES (
                '{self.record.datetime_server}',
                '{self.record.datetime_server}',
                '{self.record.datetime_server}',
                '{self.record.datetime_device}',
                {self.record.patient_id},
                '{self.record.patient_identification}',
                {self.record.device_id},
                '{self.record.device_identifier}',
                {self.record.variable_id},
                '{self.record.variable_name}',
                {self.record.value},
                '{self.record.payload}',
                {self.record.alarm_settings_fk_id if self.is_alarm else 'NULL'},
                {"'" + self.record.alarm_name + "'" if self.is_alarm else "''"},
                {"'" + self.record.alarm_operator + "'" if self.is_alarm else "''"},
                {self.record.alarm_ref_value if self.is_alarm else 'NULL'}
            );
        """
        self.db_connector.insert(query)
