from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime

Device = namedtuple('Device', ['id', 'patient_id'])
Variable = namedtuple('Variable', ['id', 'name', 'identifier'])
AlarmSettings = namedtuple('AlarmSettings', ['id', 'name', 'reference_value', 'operator'])
Patient = namedtuple('Patient', ['id', 'identification'])


@dataclass
class Record:
    datetime_server: datetime
    datetime_device: datetime
    patient_id: int
    patient_identification: str
    device_id: int
    device_identifier: str
    variable_id: int
    variable_name: str
    value: float = 0.0
    payload: str = ''
    is_alarm: bool = False
    alarm_settings_fk_id: int = None
    alarm_name: str = ''
    alarm_operator: str = ''
    alarm_ref_value: float = None
