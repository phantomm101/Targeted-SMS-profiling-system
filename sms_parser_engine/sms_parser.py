import re
import logging
from datetime import datetime
from typing import Dict, Optional, List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SMSParser:
    def __init__(self):
        # Define multiple SMS patterns for different banks/services
        self.patterns = {
            'omr_transaction': r'OMR (\d+\.?\d*) at (.+?) on (\d{4}-\d{2}-\d{2})',
            'bank_debit': r'Debit: OMR (\d+\.?\d*) from (.+?) on (\d{2}/\d{2}/\d{4})',
            'mobile_money': r'You sent OMR (\d+\.?\d*) to (.+?) on (\d{4}-\d{2}-\d{2})',
            'generic_amount': r'(\d+\.?\d*) OMR'
        }
    
    def parse_sms(self, sms: Dict) -> Dict:
        """
        Parse SMS message and extract transaction information
        
        Args:
            sms (Dict): SMS data containing 'body', 'phone', 'timestamp'
            
        Returns:
            Dict: Parsed transaction data or empty dict if parsing fails
        """
        try:
            # Validate input
            if not isinstance(sms, dict):
                logger.error("SMS input must be a dictionary")
                return {}
            
            body = sms.get("body", "").strip()
            phone = sms.get("phone", "").strip()
            timestamp = sms.get("timestamp", "")
            
            if not body:
                logger.warning("Empty SMS body received")
                return {}
            
            if not phone:
                logger.warning("No phone number provided")
                return {}
            
            # Try different parsing patterns
            parsed_data = None
            
            # Pattern 1: OMR transaction format
            parsed_data = self._parse_omr_transaction(body, phone, timestamp)
            if parsed_data:
                return parsed_data
            
            # Pattern 2: Bank debit format
            parsed_data = self._parse_bank_debit(body, phone, timestamp)
            if parsed_data:
                return parsed_data
            
            # Pattern 3: Mobile money format
            parsed_data = self._parse_mobile_money(body, phone, timestamp)
            if parsed_data:
                return parsed_data
            
            # Pattern 4: Generic amount extraction
            parsed_data = self._parse_generic_amount(body, phone, timestamp)
            if parsed_data:
                return parsed_data
            
            logger.warning(f"Could not parse SMS: {body[:50]}...")
            return {}
            
        except Exception as e:
            logger.error(f"Error parsing SMS: {str(e)}")
            return {}
    
    def _parse_omr_transaction(self, body: str, phone: str, timestamp: str) -> Optional[Dict]:
        """Parse OMR transaction format"""
        match = re.search(self.patterns['omr_transaction'], body)
        if match:
            try:
                return {
                    "phone": phone,
                    "amount": float(match.group(1)),
                    "vendor": match.group(2).strip(),
                    "date": self._parse_date(match.group(3)),
                    "transaction_type": "purchase",
                    "source": "bank_sms",
                    "original_message": body,
                    "timestamp": timestamp or datetime.now().isoformat()
                }
            except (ValueError, IndexError) as e:
                logger.error(f"Error parsing OMR transaction: {e}")
                return None
        return None
    
    def _parse_bank_debit(self, body: str, phone: str, timestamp: str) -> Optional[Dict]:
        """Parse bank debit format"""
        match = re.search(self.patterns['bank_debit'], body)
        if match:
            try:
                date_str = match.group(3)
                parsed_date = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
                return {
                    "phone": phone,
                    "amount": float(match.group(1)),
                    "vendor": match.group(2).strip(),
                    "date": parsed_date,
                    "transaction_type": "debit",
                    "source": "bank_sms",
                    "original_message": body,
                    "timestamp": timestamp or datetime.now().isoformat()
                }
            except (ValueError, IndexError) as e:
                logger.error(f"Error parsing bank debit: {e}")
                return None
        return None
    
    def _parse_mobile_money(self, body: str, phone: str, timestamp: str) -> Optional[Dict]:
        """Parse mobile money format"""
        match = re.search(self.patterns['mobile_money'], body)
        if match:
            try:
                return {
                    "phone": phone,
                    "amount": float(match.group(1)),
                    "vendor": match.group(2).strip(),
                    "date": self._parse_date(match.group(3)),
                    "transaction_type": "transfer",
                    "source": "mobile_money",
                    "original_message": body,
                    "timestamp": timestamp or datetime.now().isoformat()
                }
            except (ValueError, IndexError) as e:
                logger.error(f"Error parsing mobile money: {e}")
                return None
        return None
    
    def _parse_generic_amount(self, body: str, phone: str, timestamp: str) -> Optional[Dict]:
        """Extract amount from any SMS containing OMR"""
        match = re.search(self.patterns['generic_amount'], body)
        if match:
            try:
                return {
                    "phone": phone,
                    "amount": float(match.group(1)),
                    "vendor": "unknown",
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "transaction_type": "unknown",
                    "source": "generic_sms",
                    "original_message": body,
                    "timestamp": timestamp or datetime.now().isoformat()
                }
            except (ValueError, IndexError) as e:
                logger.error(f"Error parsing generic amount: {e}")
                return None
        return None
    
    def _parse_date(self, date_str: str) -> str:
        """Parse date string to ISO format"""
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").isoformat()
        except ValueError:
            logger.warning(f"Could not parse date: {date_str}")
            return datetime.now().isoformat()

# Usage example
parser = SMSParser()