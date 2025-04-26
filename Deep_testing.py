import requests
import warnings
import pandas as pd
from bs4 import BeautifulSoup
import hashlib

warnings.filterwarnings("ignore")

def generate_url(case_no):
    base_url1 = "http://cdsinfo.cadence.com/cgi-bin/cdsinfoprod?input="
    base_url2 = "&type=_&codmode=p"
    generated_url = base_url1 + str(case_no) + base_url2
    return generated_url

def fetch_url_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return None

def parse_case(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')

    # Extract Case Information
    case_number_tag = soup.find('font', text=lambda x: x and 'Case Number' in x)
    case_number = case_number_tag.find_next('font').get_text(strip=True) if case_number_tag else ''

    case_title_tag = case_number_tag.find_next('b') if case_number_tag else None
    case_title = case_title_tag.get_text(strip=True) if case_title_tag else ''

    env_tag = soup.find('font', text=lambda x: x and 'Environment' in x)
    environment = env_tag.find_next('b').get_text(strip=True) if env_tag else ''

    # Extract Summary
    summary_section = soup.find('h4', text='Summary')
    case_summary = ''
    if summary_section:
        summary_content = summary_section.find_next('td', {'colspan': '3'})
        if summary_content:
            case_summary = summary_content.get_text(separator=' ', strip=True)

    # Extract Product Information
    product_info = {}
    product_section = soup.find('h4', text='Product Information')
    if product_section:
        table = product_section.find_next('table')
        if table:
            rows = table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    key = cols[0].get_text(strip=True)
                    val = cols[1].get_text(strip=True)
                    product_info[key] = val

    # Extract Contact Information
    contact_info = {}
    contact_section = soup.find('h4', text='Contact Information')
    if contact_section:
        table = contact_section.find_next('table')
        if table:
            rows = table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    key = cols[0].get_text(strip=True)
                    val = cols[1].get_text(strip=True)
                    contact_info[key] = val

    # Emails Section
    emails_data = []
    hashed_emails = set()

    emails_section = soup.find('b', text='Emails')
    if emails_section:
        emails_table = emails_section.find_next('table')
        if emails_table:
            email_rows = emails_table.find_all('tr')
            for row in email_rows:
                cells = row.find_all('td')
                if len(cells) == 5:
                    email_name = cells[0].get_text(strip=True)
                    email_status = cells[1].get_text(strip=True)
                    email_subject = cells[2].get_text(strip=True)
                    email_from = cells[3].get_text(strip=True)
                    email_date = cells[4].get_text(strip=True)

                    # Email body parsing
                    body_table = row.find_next('table')
                    email_body = ''
                    if body_table:
                        body_div = body_table.find('div')
                        if body_div:
                            email_body = body_div.get_text(separator=' ', strip=True)

                    email_hash = hashlib.md5(email_body.encode()).hexdigest()

                    if email_hash not in hashed_emails:
                        hashed_emails.add(email_hash)
                        emails_data.append({
                            'Case Number': case_number,
                            'Case Title': case_title,
                            'Environment': environment,
                            'Case Summary': case_summary,
                            'Email Name': email_name,
                            'Email Status': email_status,
                            'Email Subject': email_subject,
                            'Email From': email_from,
                            'Email Date': email_date,
                            'Email Body': email_body,
                            **product_info,
                            **contact_info
                        })
    return emails_data

def main():
    # List your case numbers here
    case_numbers = ['46816635']

    all_cases_data = []

    for case_no in case_numbers:
        print(f"Processing Case: {case_no}")
        url = generate_url(case_no)
        html_content = fetch_url_content(url)
        if html_content:
            case_data = parse_case(html_content)
            all_cases_data.extend(case_data)

    # Save to CSV
    if all_cases_data:
        df = pd.DataFrame(all_cases_data)
        df.to_csv('final_cases_output.csv', index=False, encoding='utf-8-sig')
        print("Scraping Completed! Data saved to 'final_cases_output.csv'")
    else:
        print("No data found to save.")

if __name__ == "__main__":
    main()
