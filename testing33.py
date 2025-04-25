import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib

def generate_url(case_no):
    return f"http://cdsinfo.cadence.com/cgi-bin/cdsinfoprod?input={case_no}&type=_&codmode=p"

def fetch_html(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error fetching URL: {e}")
        return None

def extract_case_info(html):
    soup = BeautifulSoup(html, 'html.parser')
    
    case_info = {
        'Case Number': '',
        'Case Title': '',
        'Case Description': '',
        'Product Class': '',
        'Status': '',
        'Severity': '',
        'Date Opened': '',
        'Date Closed': ''
    }

    try:
        case_info['Case Number'] = soup.find('span', string='Case Number').find_next('span').text.strip()
    except AttributeError:
        pass

    try:
        case_info['Case Title'] = soup.find('b').text.strip()
    except AttributeError:
        pass

    try:
        case_info['Case Description'] = soup.find('span', string='Description').find_next('span').text.strip()
    except AttributeError:
        pass

    try:
        case_info['Product Class'] = soup.find('span', string='Product Class').find_next('span').text.strip()
    except AttributeError:
        pass

    try:
        case_info['Status'] = soup.find('span', string='Status').find_next('span').text.strip()
    except AttributeError:
        pass

    try:
        case_info['Severity'] = soup.find('span', string='Severity').find_next('span').text.strip()
    except AttributeError:
        pass

    try:
        case_info['Date Opened'] = soup.find('span', string='Date/Time Opened').find_next('span').text.strip()
    except AttributeError:
        pass

    try:
        case_info['Date Closed'] = soup.find('span', string='Date/Time Closed').find_next('span').text.strip()
    except AttributeError:
        pass

    return case_info

def parse_email_table(table):
    emails = []
    seen_hashes = set()
    
    rows = table.find_all('tr')[2:]  # Skip header rows
    for row in rows:
        cols = row.find_all('td')
        if len(cols) >= 5:
            email = {
                'Email Name': cols[0].text.strip(),
                'Status': cols[1].text.strip(),
                'Subject': cols[2].text.strip(),
                'FromAddress': cols[3].text.strip(),
                'MessageDate': cols[4].text.strip(),
                'Content': ''
            }

            # Extract email content
            content_section = row.find_next('tr').find('div')
            if content_section:
                email['Content'] = ' '.join(content_section.stripped_strings)

            # Create unique hash to detect duplicates
            email_hash = hashlib.md5(
                (email['Subject'] + email['MessageDate'] + email['Content']).encode()
            ).hexdigest()

            if email_hash not in seen_hashes:
                seen_hashes.add(email_hash)
                emails.append(email)
    
    return emails

def extract_emails(html):
    soup = BeautifulSoup(html, 'html.parser')
    emails_section = soup.find('b', string='Emails')
    
    if not emails_section:
        return []
    
    email_tables = emails_section.find_parent('tr').find_next_siblings('tr')[1].find_all('table')
    all_emails = []
    
    for table in email_tables:
        all_emails.extend(parse_email_table(table))
    
    return all_emails

def process_case(case_no):
    url = generate_url(case_no)
    html = fetch_html(url)
    
    if not html:
        return pd.DataFrame()

    case_info = extract_case_info(html)
    emails = extract_emails(html)

    # Create DataFrame
    df = pd.DataFrame(emails)
    for key, value in case_info.items():
        df[key] = value
    
    # Reorder columns
    columns = ['Case Number', 'Case Title', 'Case Description', 'Product Class',
               'Status', 'Severity', 'Date Opened', 'Date Closed',
               'Email Name', 'Status', 'Subject', 'FromAddress', 
               'MessageDate', 'Content']
    
    return df[columns]

def save_to_csv(df, filename):
    df.to_csv(filename, index=False, encoding='utf-8-sig')

if __name__ == "__main__":
    case_number = '46816635'
    result_df = process_case(case_number)
    
    if not result_df.empty:
        save_to_csv(result_df, 'case_report.csv')
        print("Report generated successfully!")
    else:
        print("Failed to generate report")
