# nlp_engine/sms_parser.py
import re
from datetime import datetime

def parse_sms(sms):
    body = sms.get("body", "")
    phone = sms.get("phone", "")
    match = re.search(r'OMR (\d+\.?\d*) at (.+?) on (\d{4}-\d{2}-\d{2})', body)
    if match:
        return {
            "phone": phone,
            "amount": float(match.group(1)),
            "vendor": match.group(2),
            "date": datetime.strptime(match.group(3), "%Y-%m-%d").isoformat()
        }
    return {}
