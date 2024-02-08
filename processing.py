from data_types import Device, Variable, AlarmSettings, Record, Patient
from decoder import Decoder


class Processing:
    payload = None
    db_connector = None

    def __init__(self, payload, db_connector):
        self.payload = payload
        self.db_connector = db_connector

    def run(self):
        decoder = Decoder(self.payload)
        if decoder.is_valid():
            decoder.decode_payload()
            device = self.get_device(decoder.device_identifier)
            if not device:
                print(f'DEVICE non existent; payload: {self.payload}')
                return
            patient = self.get_patient(device.patient_id)
            if not patient:
                print(f'NO PATIENT is assigned to device; payload: {self.payload}')
                return
            for variable_identifier, value in decoder.variables_data.items():
                try:
                    variable = self.get_variable(variable_identifier)
                    if not variable:
                        print(f'VARIABLE {variable_identifier} non existent; payload: {self.payload}')
                        continue
                    record = Record(
                        device_identifier=decoder.device_identifier,
                        datetime_device=decoder.device_datetime,
                        datetime_server=decoder.server_datetime,
                        payload=decoder.payload,
                        value=value,
                        device_id=device.id,
                        variable_id=variable.id,
                        variable_name=variable.name,
                        patient_id=patient.id,
                        patient_identification=patient.identification
                    )

                    alarm_settings = self.get_alarm_settings(patient.id, variable.id)
                    record.is_alarm = self.get_is_alarm(value, alarm_settings)
                    if record.is_alarm:
                        record.alarm_settings_fk_id = alarm_settings.id
                        record.alarm_name = alarm_settings.name
                        record.alarm_operator = alarm_settings.operator
                        record.alarm_ref_value = alarm_settings.reference_value
                    self.save(record)
                except Exception as e:
                    print(f'Exception for variable {variable_identifier}: {e}')
        else:
            print(f'PAYLOAD NOT VALID; payload: {self.payload}')
            return

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

    def save(self, record):
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
                '{record.datetime_server}',
                '{record.datetime_server}',
                '{record.datetime_server}',
                '{record.datetime_device}',
                {record.patient_id},
                '{record.patient_identification}',
                {record.device_id},
                '{record.device_identifier}',
                {record.variable_id},
                '{record.variable_name}',
                {record.value},
                '{record.payload}',
                {record.alarm_settings_fk_id if record.is_alarm else 'NULL'},
                {"'" + record.alarm_name + "'" if record.is_alarm else "''"},
                {"'" + record.alarm_operator + "'" if record.is_alarm else "''"},
                {record.alarm_ref_value if record.is_alarm else 'NULL'}
            );
        """
        self.db_connector.insert(query)
