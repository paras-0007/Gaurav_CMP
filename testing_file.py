import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# Constants
CSV_COLUMNS = ['Type', 'Status', 'From', 'Subject', 'Date', 'Days Since Last', 'Content']

def generate_url(case_no):
    return f"http://cdsinfo.cadence.com/cgi-bin/cdsinfoprod?input={case_no}&type=_&codmode=p"

def fetch_html(url):
    try:
        return requests.get(url).text
    except Exception as e:
        print(f"Connection Error: {e}")
        return None

def parse_date(date_str):
    formats = [
        '%m/%d/%Y, %H:%M:%S',  # 07/30/2024, 10:17:05
        '%d %B %Y at %H:%M',    # 31 July 2024 at 14:30
        '%Y-%m-%d %H:%M:%S',    # 2024-07-30 10:17:05
        '%d/%m/%Y, %H:%M:%S'    # 30/07/2024, 10:17:05
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

def extract_comms_data(html):
    soup = BeautifulSoup(html, 'html.parser')
    data = []

    # Extract Email Communications
    emails_section = soup.find('h3', string='COMMUNICATIONS:')
    if emails_section:
        table = emails_section.find_next('table')
        for row in table.find_all('tr')[1:]:  # Skip header
            cols = row.find_all('td')
            if len(cols) >= 5:
                comm_type = cols[0].get_text(strip=True)
                sender = cols[1].get_text(strip=True)
                subject = cols[2].get_text(strip=True)
                date = cols[3].get_text(strip=True)
                days = cols[4].get_text(strip=True)
                content = row.find_next('tr').td.get_text(strip=True) if row.find_next('tr') else ""
                
                data.append({
                    'Type': comm_type,
                    'Status': 'Received',
                    'From': sender,
                    'Subject': subject,
                    'Date': date,
                    'Days Since Last': days,
                    'Content': content
                })

    # Extract Case Information
    case_info = {}
    info_section = soup.find('h3', string='Case Information:')
    if info_section:
        for p in info_section.find_all_next('p'):
            if ':' in p.text:
                key, val = p.text.split(':', 1)
                case_info[key.strip()] = val.strip()
    
    # Extract Comments
    comments_section = soup.find('h2', string='Case Comments')
    if comments_section:
        for comment in comments_section.find_next('table').find_all('tr')[1:]:
            cols = comment.find_all('td')
            if len(cols) >= 3:
                data.append({
                    'Type': 'COMMENT',
                    'Status': 'Posted',
                    'From': cols[1].get_text(strip=True),
                    'Subject': 'Case Comment',
                    'Date': cols[2].get_text(strip=True),
                    'Days Since Last': 'N/A',
                    'Content': cols[0].get_text(strip=True)
                })

    # Extract CCR Information
    ccr_section = soup.find('h2', string='CCR Information')
    if ccr_section:
        ccr_content = ccr_section.find_next('div', class_='description')
        data.append({
            'Type': 'CCR',
            'Status': 'Documented',
            'From': 'Cadence Support',
            'Subject': 'Change Control Request',
            'Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Days Since Last': 'N/A',
            'Content': ccr_content.get_text(strip=True) if ccr_content else ""
        })

    return pd.DataFrame(data), case_info

def process_case(case_no):
    url = generate_url(case_no)
    html = fetch_html(url)
    if not html:
        return None

    # Extract and structure data
    comms_df, case_info = extract_comms_data(html)
    
    # Add case metadata
    comms_df['Case Number'] = case_no
    comms_df['Case Title'] = case_info.get('Case Title', '')
    comms_df['Product'] = case_info.get('Product', '')
    
    # Clean date formatting
    comms_df['Parsed Date'] = comms_df['Date'].apply(parse_date)
    comms_df.sort_values('Parsed Date', inplace=True)
    
    # Reorder columns
    final_cols = ['Case Number', 'Case Title', 'Product'] + CSV_COLUMNS
    return comms_df[final_cols].drop(columns=['Parsed Date'])

# Main Execution
if __name__ == "__main__":
    case_number = "46816635"
    result_df = process_case(case_number)
    
    if result_df is not None:
        # Save to CSV with enhanced formatting
        result_df.to_csv(f'case_{case_number}_structured.csv', 
                        index=False,
                        encoding='utf-8-sig',
                        date_format='%Y-%m-%d %H:%M:%S')
        print(f"Successfully generated structured report for case {case_number}")
    else:
        print("Failed to generate report")