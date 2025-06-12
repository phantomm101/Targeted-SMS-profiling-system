import re
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BankSMSParser:
    """
    Advanced SMS parser for bank statements supporting multiple banks and transaction types.
    Handles debit, credit, balance inquiries, and various transaction formats.
    """
    
    def __init__(self):
        # Common currency patterns
        self.currency_patterns = {
            'USD': r'(?:USD|\$)\s*(\d+(?:,\d{3})*(?:\.\d{2})?)',
            'EUR': r'(?:EUR|€)\s*(\d+(?:,\d{3})*(?:\.\d{2})?)',
            'GBP': r'(?:GBP|£)\s*(\d+(?:,\d{3})*(?:\.\d{2})?)',
            'OMR': r'(?:OMR|RO)\s*(\d+(?:,\d{3})*(?:\.\d{2,3})?)',
            'AED': r'(?:AED|DHS?)\s*(\d+(?:,\d{3})*(?:\.\d{2})?)',
            'SAR': r'(?:SAR|SR)\s*(\d+(?:,\d{3})*(?:\.\d{2})?)',
            'INR': r'(?:INR|Rs\.?)\s*(\d+(?:,\d{3})*(?:\.\d{2})?)',
            'KES': r'(?:KES|KSh)\s*(\d+(?:,\d{3})*(?:\.\d{2})?)',
            'GENERIC': r'(\d+(?:,\d{3})*(?:\.\d{2,3})?)'
        }
        
        # Date patterns
        self.date_patterns = [
            r'(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})',  # DD-MM-YYYY or MM/DD/YYYY
            r'(\d{4}[-/]\d{1,2}[-/]\d{1,2})',    # YYYY-MM-DD
            r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})',  # DD MMM YYYY
            r'(\d{1,2}(?:st|nd|rd|th)?\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})'
        ]
        
        # Time patterns
        self.time_patterns = [
            r'(\d{1,2}:\d{2}(?::\d{2})?(?:\s*(?:AM|PM))?)',
            r'at\s+(\d{1,2}:\d{2}(?::\d{2})?)',
            r'(\d{2}:\d{2})'
        ]
        
        # Transaction type keywords
        self.transaction_types = {
            'debit': ['debited', 'debit', 'withdrawn', 'withdraw', 'paid', 'purchase', 'spent', 'charged'],
            'credit': ['credited', 'credit', 'deposited', 'deposit', 'received', 'salary', 'refund', 'cashback'],
            'transfer': ['transferred', 'transfer', 'sent to', 'received from'],
            'fee': ['fee', 'charge', 'charges', 'service charge', 'maintenance'],
            'atm': ['ATM', 'cash withdrawal', 'ATM withdrawal'],
            'pos': ['POS', 'card payment', 'swipe'],
            'online': ['online', 'internet banking', 'mobile banking', 'UPI', 'NEFT', 'RTGS', 'IMPS']
        }
    
    def clean_amount(self, amount_str: str) -> float:
        """Clean and convert amount string to float"""
        if not amount_str:
            return 0.0
        
        # Remove commas and extra spaces
        cleaned = re.sub(r'[,\s]', '', amount_str)
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    
    def parse_date(self, text: str) -> Optional[str]:
        """Extract and standardize date from SMS text"""
        for pattern in self.date_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                # Try to parse and standardize the date
                try:
                    # Handle different date formats
                    for fmt in ['%d-%m-%Y', '%d/%m/%Y', '%Y-%m-%d', '%Y/%m/%d', 
                               '%d-%m-%y', '%d/%m/%y', '%d %b %Y', '%d %B %Y']:
                        try:
                            parsed_date = datetime.strptime(date_str, fmt)
                            return parsed_date.strftime('%Y-%m-%d')
                        except ValueError:
                            continue
                except:
                    pass
        
        # If no date found, assume today
        return datetime.now().strftime('%Y-%m-%d')
    
    def parse_time(self, text: str) -> Optional[str]:
        """Extract time from SMS text"""
        for pattern in self.time_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                time_str = match.group(1)
                try:
                    # Standardize time format
                    if 'AM' in time_str.upper() or 'PM' in time_str.upper():
                        parsed_time = datetime.strptime(time_str, '%I:%M %p')
                    else:
                        parsed_time = datetime.strptime(time_str, '%H:%M')
                    return parsed_time.strftime('%H:%M:%S')
                except ValueError:
                    return time_str
        return None
    
    def extract_amount_and_currency(self, text: str) -> Dict[str, Union[float, str]]:
        """Extract amount and currency from text"""
        for currency, pattern in self.currency_patterns.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                amount = self.clean_amount(match.group(1))
                return {
                    'amount': amount,
                    'currency': currency if currency != 'GENERIC' else 'USD'
                }
        
        # Fallback: look for standalone numbers that might be amounts
        amount_match = re.search(r'\b(\d+(?:,\d{3})*(?:\.\d{2,3})?)\b', text)
        if amount_match:
            return {
                'amount': self.clean_amount(amount_match.group(1)),
                'currency': 'USD'  # Default currency
            }
        
        return {'amount': 0.0, 'currency': 'USD'}
    
    def determine_transaction_type(self, text: str) -> str:
        """Determine transaction type from SMS text"""
        text_lower = text.lower()
        
        for trans_type, keywords in self.transaction_types.items():
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    return trans_type
        
        # Default classification based on common indicators
        if any(word in text_lower for word in ['spent', 'paid', 'purchase']):
            return 'debit'
        elif any(word in text_lower for word in ['received', 'deposited']):
            return 'credit'
        
        return 'unknown'
    
    def extract_merchant_or_description(self, text: str) -> str:
        """Extract merchant name or transaction description"""
        # Common patterns for merchant extraction
        merchant_patterns = [
            r'(?:at|to|from)\s+([A-Z][A-Z\s&\-\.]+?)(?:\s+on|\s+dated|\s+\d|$)',
            r'(?:POS|Card)\s+([A-Z][A-Z\s&\-\.]+?)(?:\s+on|\s+dated|\s+\d|$)',
            r'(?:paid\s+to|sent\s+to)\s+([A-Z][A-Z\s&\-\.]+?)(?:\s+on|\s+dated|\s+\d|$)',
            r'(?:from|to)\s+([A-Z][A-Z\s&\-\.]{3,}?)(?:\s+on|\s+dated|\s+\d|$)'
        ]
        
        for pattern in merchant_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                merchant = match.group(1).strip()
                # Clean up merchant name
                merchant = re.sub(r'\s+', ' ', merchant)
                if len(merchant) > 3:  # Minimum length check
                    return merchant
        
        # Fallback: extract any capitalized words that might be merchant names
        words = text.split()
        merchant_words = []
        for i, word in enumerate(words):
            if word[0].isupper() and len(word) > 2 and not word.isdigit():
                # Look for consecutive capitalized words
                j = i
                while j < len(words) and words[j][0].isupper() and not words[j].isdigit():
                    merchant_words.append(words[j])
                    j += 1
                if len(merchant_words) >= 1:
                    return ' '.join(merchant_words[:4])  # Limit to 4 words
        
        return 'Unknown Merchant'
    
    def extract_balance(self, text: str) -> Optional[float]:
        """Extract account balance from SMS"""
        balance_patterns = [
            r'(?:balance|bal|available)\s*(?:is|:)?\s*(?:USD|\$|OMR|RO|AED|EUR|£|Rs\.?|KSh)?\s*(\d+(?:,\d{3})*(?:\.\d{2,3})?)',
            r'(?:remaining|current)\s*(?:balance|bal)\s*(?:is|:)?\s*(?:USD|\$|OMR|RO|AED|EUR|£|Rs\.?|KSh)?\s*(\d+(?:,\d{3})*(?:\.\d{2,3})?)',
            r'(?:USD|\$|OMR|RO|AED|EUR|£|Rs\.?|KSh)\s*(\d+(?:,\d{3})*(?:\.\d{2,3})?)\s*(?:balance|bal|remaining)'
        ]
        
        for pattern in balance_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return self.clean_amount(match.group(1))
        
        return None
    
    def extract_reference_number(self, text: str) -> Optional[str]:
        """Extract transaction reference number"""
        ref_patterns = [
            r'(?:ref|reference|txn|transaction)\s*(?:no|number|id|#)?\s*:?\s*([A-Z0-9]{6,})',
            r'(?:UPI|NEFT|RTGS|IMPS)\s*(?:ref|id)\s*:?\s*([A-Z0-9]{6,})',
            r'([A-Z0-9]{12,16})'  # Generic long alphanumeric string
        ]
        
        for pattern in ref_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
    
    def parse_sms(self, sms_data: Dict) -> Dict:
        """
        Main parsing function that extracts structured data from bank SMS
        
        Args:
            sms_data: Dictionary containing 'body', 'phone', 'timestamp' etc.
            
        Returns:
            Dictionary with parsed transaction details
        """
        body = sms_data.get('body', '')
        sender = sms_data.get('phone', sms_data.get('sender', ''))
        timestamp = sms_data.get('timestamp', datetime.now().isoformat())
        
        if not body:
            return {'error': 'Empty SMS body'}
        
        # Extract basic transaction information
        amount_info = self.extract_amount_and_currency(body)
        transaction_type = self.determine_transaction_type(body)
        merchant = self.extract_merchant_or_description(body)
        date = self.parse_date(body)
        time = self.parse_time(body)
        balance = self.extract_balance(body)
        reference = self.extract_reference_number(body)
        
        # Create structured output
        parsed_data = {
            'raw_sms': body,
            'sender': sender,
            'parsed_timestamp': timestamp,
            'transaction': {
                'amount': amount_info['amount'],
                'currency': amount_info['currency'],
                'type': transaction_type,
                'merchant': merchant,
                'date': date,
                'time': time,
                'reference_number': reference
            },
            'account': {
                'balance': balance
            },
            'metadata': {
                'parsing_confidence': self.calculate_confidence(body, amount_info, transaction_type),
                'parsed_at': datetime.now().isoformat()
            }
        }
        
        return parsed_data
    
    def calculate_confidence(self, text: str, amount_info: Dict, transaction_type: str) -> float:
        """Calculate parsing confidence score (0-1)"""
        confidence = 0.0
        
        # Amount detection confidence
        if amount_info['amount'] > 0:
            confidence += 0.3
        
        # Transaction type confidence
        if transaction_type != 'unknown':
            confidence += 0.2
        
        # Date presence
        if any(re.search(pattern, text, re.IGNORECASE) for pattern in self.date_patterns):
            confidence += 0.2
        
        # Currency detection
        if amount_info['currency'] != 'USD':  # Non-default currency detected
            confidence += 0.1
        
        # Reference number presence
        if self.extract_reference_number(text):
            confidence += 0.1
        
        # Balance information
        if self.extract_balance(text):
            confidence += 0.1
        
        return min(confidence, 1.0)
    
    def batch_parse(self, sms_list: List[Dict]) -> List[Dict]:
        """Parse multiple SMS messages"""
        results = []
        for sms in sms_list:
            try:
                parsed = self.parse_sms(sms)
                results.append(parsed)
            except Exception as e:
                logger.error(f"Error parsing SMS: {e}")
                results.append({'error': str(e), 'raw_sms': sms.get('body', '')})
        
        return results

# Example usage and test cases
if __name__ == "__main__":
    parser = BankSMSParser()
    
    # Sample SMS messages from different banks
    sample_sms_messages = [
        {
            "body": "Your account has been debited with USD 45.20 at STARBUCKS CAFE on 2024-06-10 at 14:30. Available balance: USD 1,234.56. Ref: TXN123456789",
            "phone": "+1234567890",
            "timestamp": "2024-06-10T14:30:00"
        },
        {
            "body": "OMR 125.50 credited to your account from SALARY DEPOSIT on 01/06/2024. Current balance: OMR 2,345.75",
            "phone": "BANK-ALERT",
            "timestamp": "2024-06-01T09:00:00"
        },
        {
            "body": "Card payment of AED 89.25 at CARREFOUR MALL DUBAI on 10-Jun-2024 15:45. Remaining balance AED 567.80",
            "phone": "90123",
            "timestamp": "2024-06-10T15:45:00"
        },
        {
            "body": "UPI transaction: Rs 250.00 paid to AMAZON INDIA. UPI Ref: 98765432101. Balance: Rs 5,430.25",
            "phone": "HDFC-BANK",
            "timestamp": "2024-06-10T12:15:00"
        },
        {
            "body": "ATM withdrawal of £50.00 at HSBC ATM LONDON on 10/06/2024 18:20. Available balance: £890.45",
            "phone": "HSBC",
            "timestamp": "2024-06-10T18:20:00"
        }
    ]
    
    # Parse sample messages
    print("=== Bank SMS Parser Test Results ===\n")
    
    for i, sms in enumerate(sample_sms_messages, 1):
        print(f"SMS {i}:")
        print(f"Original: {sms['body']}")
        
        parsed = parser.parse_sms(sms)
        print(f"Parsed Result:")
        print(json.dumps(parsed, indent=2, default=str))
        print("-" * 80)
    
    # Batch parsing example
    print("\n=== Batch Parsing Results ===")
    batch_results = parser.batch_parse(sample_sms_messages)
    
    for result in batch_results:
        if 'error' not in result:
            txn = result['transaction']
            print(f"Amount: {txn['currency']} {txn['amount']}")
            print(f"Type: {txn['type']}")
            print(f"Merchant: {txn['merchant']}")
            print(f"Date: {txn['date']}")
            print(f"Confidence: {result['metadata']['parsing_confidence']:.2f}")
            print()