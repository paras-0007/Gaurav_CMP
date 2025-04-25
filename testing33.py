import requests
import warnings
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np

warnings.filterwarnings("ignore")

def generate_url(case_no):
    base_url1 = "http://cdsinfo.cadence.com/cgi-bin/cdsinfoprod?input="
    base_url2 = "&type=_&codmode=p"
    return f"{base_url1}{case_no}{base_url2}"

def fetch_url_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return None

def extract_case_info(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Extract case number, title, and description
    case_number = soup.find(text="Case Number").find_next().text.strip()
    case_title = soup.find(text="Case Title").find_next().text.strip()
    case_description = soup.find(text="Description").find_next().find_next().text.strip()
    
    return case_number, case_title, case_description

def extract_emails(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    email_section = soup.find(text="Emails").find_parent('table')
    email_rows = email_section.find_all('tr')[1:]  # Skip header row

    emails = []
    seen_hashes = set()  # To avoid duplicates

    for row in email_rows:
        cols = row.find_all('td')
        if len(cols) < 5:
            continue  # Skip rows that don't have enough columns

        email_name = cols[0].text.strip()
        status = cols[1].text.strip()
        subject = cols[2].text.strip()
        from_address = cols[3].text.strip()
        message_date = cols[4].text.strip()

        # Create a unique identifier for the email entry
        email_hash = (email_name, subject, from_address, message_date)
        if email_hash not in seen_hashes:
            seen_hashes.add(email_hash)
            emails.append({
                "Email Name": email_name,
                "Status": status,
                "Subject": subject,
                "From Address": from_address,
                "Message Date": message_date
            })

    return emails

def extract_case_summary(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    summary_section = soup.find(text="Case Summary").find_next().find_next()
    return summary_section.text.strip()

def generate_csv(case_no):
    url = generate_url(case_no)
    html_content = fetch_url_content(url)

    if not html_content:
        return "Error fetching content from URL."

    case_number, case_title, case_description = extract_case_info(html_content)
    emails = extract_emails(html_content)
    case_summary = extract_case_summary(html_content)

    # Create a DataFrame for the CSV output
    data = {
        "Case Number": [case_number],
        "Case Title": [case_title],
        "Case Description": [case_description],
        "Case Summary": [case_summary]
    }

    # Expand email data into the DataFrame
    for email in emails:
        for key in email:
            data.setdefault(key, []).append(email[key])

    # Create DataFrame and save to CSV
    df = pd.DataFrame(data)
    df.to_csv(f'case_{case_number}.csv', index=False)

# Example usage
generate_csv('46816635')