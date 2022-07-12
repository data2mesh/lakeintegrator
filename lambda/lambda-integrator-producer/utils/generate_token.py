import uuid
import datetime

def generate_token():
    now = datetime.datetime.utcnow()
    audit_token = {
        "uuid_file" : uuid.uuid4().hex,
        "process_code" : now.strftime("%Y%m%d%H%M%S"),
        "pt_year" : now.strftime("%Y"),
        "pt_month" : now.strftime("%m"),
        "pt_day" : now.strftime("%d"),
        "pt_hour" : now.strftime("%H"),
        "pt_minute" : now.strftime("%M"),
        "pt_second" : now.strftime("%S"),
        "pt_time" : now.strftime("%H%M%S")
    }
    return audit_token
