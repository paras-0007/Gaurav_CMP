import requests
import warnings
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np
import re

warnings.filterwarnings("ignore")

def generate_url(case_no):
    return f"http://cdsinfo.cadence.com/cgi-bin/cdsinfoprod?input={case_no}&type=_&codmode=p"

def fetch_url_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return None

def case_title(html_content):
    start = "Would you like to associate an Article to this Case"
    end = "Environment"
    position = html_content.find(start)
    if position != -1:
        new_content = html_content[position + len(start):]
        end_pos = new_content.find(end)
        if end_pos != -1:
            new_content2 = new_content[:end_pos]
            return BeautifulSoup(new_content2, 'html.parser').get_text(strip=True)[19:]
    return ""

def extract_description(html_content):
    start = "Description"
    end = "Severity"
    position = html_content.find(start)
    if position != -1:
        new_content = html_content[position:]
        position = new_content.find(end)
        if position != -1:
            new_content2 = new_content[:position]
            return BeautifulSoup(new_content2, 'html.parser').get_text()[12:]
    return ""

def parse_date(date_str):
    formats = ['%m/%d/%Y, %H:%M:%S','%d/%m/%Y, %H:%M:%S', '%d/%m/%Y %H:%M', '%Y/%d/%m %H:%M', '%Y-%m-%d %H:%M:%S',
               '%A, %d %B %Y at %I:%M %p', '%A, %d %B %Y at %H:%M', '%d %B %Y %H:%M:%S']
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

def index_local_to_csv(id, txt_path, csv_path):
    try:
        # Read structured text data
        with open(txt_path, 'r', encoding='utf-8') as f:
            data = f.read()

        # Extract case info (already known)
        case_info_match = re.search(r"Case Number: (\d+)\nCase Title:\n(.*?)\nCase Decription:\n(.*?)\n", data, re.DOTALL)
        case_number = case_info_match.group(1).strip()
        case_title = case_info_match.group(2).strip()
        case_description = case_info_match.group(3).strip()

        # Extract emails
        email_blocks = re.findall(r"Email-\d+.*?Date: (.*?)\s*(From:.*?)\s*(.*?)Thanks,.*?(?=Email-\d+|\Z)", data, re.DOTALL)
        emails_list = []
        for i, (date, header, body) in enumerate(email_blocks):
            from_match = re.search(r"From:\s*(\S+@\S+)", header)
            subject_match = re.search(r"Subject:\s*(.*?)\n", header)
            from_address = from_match.group(1) if from_match else "Unknown"
            subject = subject_match.group(1).strip() if subject_match else "No Subject"
            emails_list.append({
                'Email Name': f'Email-{i+1}',
                'Status': 'Initial Mail' if i == 0 else f'Response {i}',
                'Subject': subject,
                'From Address': from_address,
                'Message Date': date.strip(),
                'Body': re.sub(r'\s+', ' ', body.strip())
            })

        # Create DataFrame and save
        df = pd.DataFrame(emails_list)
        df.to_csv(csv_path, index=False)
        print(f"✅ CSV saved at: {csv_path}")

    except Exception as e:
        print(f"❌ Error during processing: {e}")

# Call function with paths to files
index_local_to_csv(
    id='46816635',
    txt_path='/mnt/data/output.txt',
    csv_path='/mnt/data/formatted_case_output.csv'
)
