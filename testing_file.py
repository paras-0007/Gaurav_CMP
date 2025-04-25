import requests
import warnings
import pandas as pd
from bs4 import BeautifulSoup
import re

warnings.filterwarnings("ignore")

def generate_url(case_no):
    """Generates the URL for a given case number."""
    base_url1 = "http://cdsinfo.cadence.com/cgi-bin/cdsinfoprod?input="
    base_url2 = "&type=_&codmode=p"
    return base_url1 + str(case_no) + base_url2

def fetch_url_content(url):
    """Fetches the HTML content of a URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for bad response status
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return None

def extract_case_details(html_content):
    """Extracts case details (number, title, summary, basics, contact, information)."""

    soup = BeautifulSoup(html_content, 'html.parser')
    details = {}

    # Case Number and Title
    case_number_title = soup.find('table', class_='collection-as-table1').find_all('font')
    details['Case Number'] = case_number_title[0].next_sibling.strip() if case_number_title else None
    details['Case Title'] = case_number_title[1].text.strip() if len(case_number_title) > 1 else None

    # Summary
    summary_section = soup.find('tr', text=re.compile(r'Summary'))
    if summary_section:
        case_summary = summary_section.find_next('td', class_='collection-as-table')
        details['Case Summary'] = case_summary.text.strip() if case_summary else None

    # Case Basics
    case_basics_section = soup.find('tr', text=re.compile(r'Case Basics'))
    if case_basics_section:
        labels = case_basics_section.find_parent('table').find_all('td', align='right')
        values = [label.find_next_sibling('td') for label in labels]
        for label, value in zip(labels, values):
            label_text = label.text.strip().replace(':', '')
            details[label_text] = value.text.strip() if value else None

    # Description
    description_section = soup.find('tr', text=re.compile(r'Description'))
    if description_section:
        description_td = description_section.find_next('td', colspan="3")
        details['Case Description'] = description_td.text.strip() if description_td else None

    # Contact Information
    contact_info_section = soup.find('tr', text=re.compile(r'Contact Information'))
    if contact_info_section:
        labels = contact_info_section.find_parent('table').find_all('td', align='right')
        values = [label.find_next_sibling('td') for label in labels]
        for label, value in zip(labels, values):
            label_text = label.text.strip().replace(':', '')
            details[label_text] = value.text.strip() if value else None

    # Case Information
    case_information_section = soup.find('tr', text=re.compile(r'Case Information'))
    if case_information_section:
        labels = case_information_section.find_parent('table').find_all('td', align='right')
        values = [label.find_next_sibling('td') for label in labels]
        for label, value in zip(labels, values):
            label_text = label.text.strip().replace(':', '')
            details[label_text] = value.text.strip() if value else None

    return details

def extract_email_headers(html_content):
    """Extracts the headers of the email section."""

    start_marker = "Emails"
    end_marker = "Open Activities"
    start_pos = html_content.find(start_marker)
    end_pos = html_content.find(end_marker)

    if start_pos == -1 or end_pos == -1:
        return ["Emails Section Not Found"]  # Return a clear message

    email_content = html_content[start_pos:end_pos]
    soup = BeautifulSoup(email_content, 'html.parser')
    header_row = soup.find('tr', style=lambda x: x and all(f'width: {w}' in x for w in ["10%", "10%", "40%", "30%", "10%"]))

    if header_row:
        return [th.text.strip() for th in header_row.find_all('td')]
    else:
        return ["Email Headers Not Found"]  # Return a clear message

def extract_emails(html_content):
    """Extracts emails from the HTML content, avoiding duplicates."""

    start_marker = "Emails"
    end_marker = "Open Activities"
    start_pos = html_content.find(start_marker)
    end_pos = html_content.find(end_marker)

    if start_pos == -1 or end_pos == -1:
        return []

    email_content = html_content[start_pos:end_pos]
    soup = BeautifulSoup(email_content, 'html.parser')

    email_data = []
    seen_contents = set()

    email_tables = soup.find_all('table', style=lambda x: x and 'width: 100%' in x and 'border: 0px' in x)

    for table in email_tables:
        row_data = {}
        header_row = table.find_previous('tr', style=lambda x: x and all(f'width: {w}' in x for w in ["10%", "10%", "40%", "30%", "10%"]))
        if header_row:
            headers = [th.text.strip() for th in header_row.find_all('td')]
        else:
            headers = ["Name", "Status", "Subject", "From Address", "Message Date"]

        data_rows = table.find_all('tr')
        if len(data_rows) > 1:  # Ensure there are data rows
            for row in data_rows[1:]:
                cells = row.find_all('td')
                if len(cells) == len(headers):
                    for i, cell in enumerate(cells):
                        row_data[headers[i]] = cell.text.strip()
                    content_hash = hash(str(row_data))
                    if content_hash not in seen_contents:
                        email_data.append(row_data)
                        seen_contents.add(content_hash)
    return email_data

def main(case_number):
    """Main function to orchestrate the scraping and data processing."""

    url = generate_url(case_number)
    html_content = fetch_url_content(url)

    if not html_content:
        return None  # Or raise an exception

    case_details = extract_case_details(html_content)
    email_headers = extract_email_headers(html_content)
    emails = extract_emails(html_content)

    # Structure the data for CSV export
    data_for_csv = [case_details]  # Start with case details
    for email in emails:
        data_for_csv.append(email)

    df = pd.DataFrame(data_for_csv)
    return df

if __name__ == "__main__":
    case_number = "46816635"  # Replace with the desired case number
    df = main(case_number)

    if df is not None:
        df.to_csv(f"case_{case_number}_report.csv", index=False)
        print(f"CSV report generated: case_{case_number}_report.csv")
    else:
        print("Failed to generate report.")