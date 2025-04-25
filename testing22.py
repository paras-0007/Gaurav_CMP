import requests
import warnings
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np
import hashlib

warnings.filterwarnings("ignore")

# Helper functions
def generate_url(case_no):
    return f"http://cdsinfo.cadence.com/cgi-bin/cdsinfoprod?input={case_no}&type=_&codmode=p"

def fetch_content(url):
    try:
        return requests.get(url).text
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

# Case metadata extraction
def extract_case_metadata(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    return {
        'case_number': extract_case_number(soup),
        'title': extract_case_title(soup),
        'description': extract_description(soup),
        'ccr': extract_ccr(soup)
    }

def extract_case_number(soup):
    return soup.find('a', string=lambda t: t and 'Case' in t).text.split()[-1]

def extract_case_title(soup):
    return soup.find('h1').get_text(strip=True)

def extract_description(soup):
    desc_tag = soup.find('h2', string='Description').find_next('div')
    return desc_tag.get_text(strip=True) if desc_tag else ''

def extract_ccr(soup):
    ccr_tag = soup.find('td', string='Bug/Enh CCR').find_next('td')
    return ccr_tag.get_text(strip=True) or 'No CCR'

# Email processing
def process_communications(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    comm_section = soup.find('h2', string='Communications').find_next('table')
    
    seen = set()
    communications = []
    
    for row in comm_section.find_all('tr')[1:]:  # Skip header
        cols = row.find_all('td')
        if len(cols) != 5:
            continue
            
        # Create unique hash for deduplication
        content_hash = hashlib.md5(str(cols).encode()).hexdigest()
        if content_hash in seen:
            continue
        seen.add(content_hash)
        
        comm = {
            'type': cols[0].get_text(strip=True),
            'status': cols[1].get_text(strip=True),
            'subject': cols[2].get_text(strip=True),
            'from_address': cols[3].get_text(strip=True).split('From:')[-1].split('To:')[0].strip(),
            'message_date': parse_date(cols[4].get_text(strip=True)),
            'content': cols[2].find_next('div').get_text(strip=True)
        }
        communications.append(comm)
    
    return communications

# Main processing
def generate_report(case_no):
    url = generate_url(case_no)
    html_content = fetch_content(url)
    if not html_content:
        return pd.DataFrame()

    metadata = extract_case_metadata(html_content)
    communications = process_communications(html_content)

    # Create dataframe
    df = pd.DataFrame(communications)
    
    # Add metadata columns
    for key, value in metadata.items():
        df[key] = value
    
    # Reorder columns
    columns = ['case_number', 'title', 'description', 'type', 'status', 
               'subject', 'from_address', 'message_date', 'content', 'ccr']
    
    return df[columns].drop_duplicates()

# Date parsing (improved)
def parse_date(date_str):
    formats = [
        '%m/%d/%Y, %H:%M:%S',
        '%d/%m/%Y, %H:%M:%S', 
        '%Y-%m-%d %H:%M:%S',
        '%A, %d %B %Y at %I:%M %p'
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d %H:%M:%S')
        except:
            continue
    return date_str

# Example usage
if __name__ == "__main__":
    report = generate_report('46816635')
    report.to_csv('case_report.csv', index=False)
    print("Report generated successfully!")