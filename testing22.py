import requests
import warnings
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np
import hashlib

warnings.filterwarnings("ignore")

# -------------------- Utility Functions --------------------
def generate_url(case_no):
    return f"http://cdsinfo.cadence.com/cgi-bin/cdsinfoprod?input={case_no}&type=_&codmode=p"

def fetch_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
        return None

def parse_date(date_str):
    formats = [
        '%m/%d/%Y, %H:%M:%S',
        '%d/%m/%Y, %H:%M:%S',
        '%Y-%m-%d %H:%M:%S',
        '%A, %d %B %Y at %I:%M %p'
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

# -------------------- Metadata Extraction --------------------
def extract_case_metadata(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    metadata = {
        'case_number': None,
        'title': None,
        'description': None,
        'ccr_number': None,
        'ccr_description': None
    }

    # Extract case number
    case_link = soup.find('a', href=lambda x: x and 'cdsinfoprod' in x)
    if case_link:
        metadata['case_number'] = case_link.text.split()[-1]

    # Extract title
    title_tag = soup.find('h1')
    if title_tag:
        metadata['title'] = title_tag.get_text(strip=True)

    # Extract description
    desc_tag = soup.find('h2', string='Description')
    if desc_tag:
        metadata['description'] = desc_tag.find_next('div').get_text(strip=True)

    # Extract CCR information
    ccr_tag = soup.find('td', string='Bug/Enh CCR')
    if ccr_tag:
        metadata['ccr_number'] = ccr_tag.find_next('td').get_text(strip=True)
        if metadata['ccr_number']:
            ccr_url = generate_url(metadata['ccr_number'])
            ccr_content = fetch_content(ccr_url)
            if ccr_content:
                ccr_soup = BeautifulSoup(ccr_content, 'html.parser')
                desc_tag = ccr_soup.find('h2', string='DESCRIPTION')
                if desc_tag:
                    metadata['ccr_description'] = desc_tag.find_next('div').get_text(strip=True)

    return metadata

# -------------------- Communications Processing --------------------
def process_communications(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    communications = []
    seen_hashes = set()

    # Process emails
    email_section = soup.find('h2', string=lambda t: t and 'Communications' in t)
    if email_section:
        email_table = email_section.find_next('table')
        for row in email_table.find_all('tr')[1:]:  # Skip header
            cols = row.find_all('td')
            if len(cols) >= 5:
                content = cols[4].get_text(strip=True)
                content_hash = hashlib.md5(content.encode()).hexdigest()
                
                if content_hash not in seen_hashes:
                    seen_hashes.add(content_hash)
                    communications.append({
                        'type': 'Email',
                        'status': cols[0].get_text(strip=True),
                        'subject': cols[1].get_text(strip=True),
                        'from_address': cols[2].get_text(strip=True),
                        'message_date': parse_date(cols[3].get_text(strip=True)),
                        'content': content
                    })

    # Process comments
    comments_section = soup.find('h2', string='Case Comments')
    if comments_section:
        comment_table = comments_section.find_next('table')
        for row in comment_table.find_all('tr')[1:]:
            cols = row.find_all('td')
            if len(cols) >= 3:
                content = cols[0].get_text(strip=True)
                content_hash = hashlib.md5(content.encode()).hexdigest()
                
                if content_hash not in seen_hashes:
                    seen_hashes.add(content_hash)
                    communications.append({
                        'type': 'Comment',
                        'status': 'N/A',
                        'subject': 'Case Comment',
                        'from_address': cols[1].get_text(strip=True),
                        'message_date': parse_date(cols[2].get_text(strip=True)),
                        'content': content
                    })

    return communications

# -------------------- Main Function --------------------
def generate_case_report(case_no):
    # Fetch and parse HTML content
    url = generate_url(case_no)
    html_content = fetch_content(url)
    if not html_content:
        return pd.DataFrame()

    # Extract data
    metadata = extract_case_metadata(html_content)
    communications = process_communications(html_content)

    # Create DataFrame
    df = pd.DataFrame(communications)
    
    # Add metadata columns
    for key, value in metadata.items():
        df[key] = value

    # Clean and format columns
    df['message_date'] = df['message_date'].apply(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else 'N/A')
    
    # Reorder columns
    column_order = [
        'case_number', 'title', 'description',
        'type', 'status', 'subject', 'from_address', 'message_date', 'content',
        'ccr_number', 'ccr_description'
    ]
    
    return df[column_order].drop_duplicates().reset_index(drop=True)

# -------------------- Execution --------------------
if __name__ == "__main__":
    case_number = '46816635'  # Replace with actual case number
    report_df = generate_case_report(case_number)
    
    if not report_df.empty:
        report_df.to_csv(f'case_{case_number}_report.csv', index=False)
        print(f"Report generated successfully for case {case_number}")
    else:
        print("Failed to generate report")
