import requests
import pandas as pd
from bs4 import BeautifulSoup
import hashlib
import warnings

warnings.filterwarnings("ignore")

def generate_url(case_no):
    base_url = "http://cdsinfo.cadence.com/cgi-bin/cdsinfoprod?input="
    return f"{base_url}{case_no}&type=_&codmode=p"

def fetch_html(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Failed to fetch URL {url}: {e}")
        return None

def hash_content(content):
    return hashlib.md5(content.encode('utf-8')).hexdigest()

def parse_case(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    parsed_data = []
    seen_hashes = set()

    # Extract Case Info
    case_number_tag = soup.find('font', string=lambda x: x and 'Case Number' in x)
    case_number = case_number_tag.find_next('font').text.strip() if case_number_tag else ''

    case_title_tag = case_number_tag.find_next('b') if case_number_tag else None
    case_title = case_title_tag.text.strip() if case_title_tag else ''

    env_tag = soup.find('font', string=lambda x: x and 'Environment' in x)
    environment = env_tag.find_next('b').text.strip() if env_tag else ''

    # Summary
    summary_section = soup.find('h4', string='Summary')
    case_summary = ''
    if summary_section:
        summary_td = summary_section.find_next('td', {'colspan': '3'})
        if summary_td:
            case_summary = summary_td.get_text(separator=' ', strip=True)

    # Product Information
    product_class = product_feature = product_version = ''
    product_section = soup.find('h4', string='Product Information')
    if product_section:
        table = product_section.find_next('table')
        if table:
            rows = table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    label = cols[0].text.strip()
                    value = cols[1].text.strip()
                    if 'Product Class' in label:
                        product_class = value
                    if 'Product Feature' in label:
                        product_feature = value
                    if 'Product Feature Version' in label:
                        product_version = value

    # Contact Information
    contact_name = contact_email = ''
    contact_section = soup.find('h4', string='Contact Information')
    if contact_section:
        table = contact_section.find_next('table')
        if table:
            rows = table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    label = cols[0].text.strip()
                    value = cols[1].text.strip()
                    if 'Contact Name' in label:
                        contact_name = value
                    if 'Contact Email' in label:
                        contact_email = value

    # Emails Section
    emails_section = soup.find('b', string='Emails')
    if emails_section:
        email_table = emails_section.find_next('table')
        if email_table:
            email_rows = email_table.find_all('tr')
            for row in email_rows:
                cols = row.find_all('td')
                if len(cols) == 5:
                    email_subject = cols[2].text.strip()
                    email_from = cols[3].text.strip()
                    email_date = cols[4].text.strip()

                    # Fetch the corresponding email body
                    next_table = row.find_next('table')
                    email_body = ''
                    if next_table:
                        body_div = next_table.find('div')
                        if body_div:
                            email_body = body_div.get_text(separator=' ', strip=True)

                    # Hash email body to avoid duplicates
                    body_hash = hash_content(email_body)
                    if body_hash not in seen_hashes:
                        seen_hashes.add(body_hash)
                        parsed_data.append({
                            'Case Number': case_number,
                            'Case Title': case_title,
                            'Environment': environment,
                            'Case Summary': case_summary,
                            'Product Class': product_class,
                            'Product Feature': product_feature,
                            'Product Version': product_version,
                            'Contact Name': contact_name,
                            'Contact Email': contact_email,
                            'Email Subject': email_subject,
                            'Email From': email_from,
                            'Email Date': email_date,
                            'Email Body': email_body
                        })
    return parsed_data

def main():
    case_numbers = ['46816635']  # Add more case numbers if needed
    all_data = []

    for case_no in case_numbers:
        print(f"Processing Case: {case_no}")
        url = generate_url(case_no)
        html_content = fetch_html(url)
        if html_content:
            case_data = parse_case(html_content)
            all_data.extend(case_data)

    if all_data:
        df = pd.DataFrame(all_data)
        df.to_csv('final_cases_output.csv', index=False, encoding='utf-8-sig')
        print("✅ Scraping completed. File saved as 'final_cases_output.csv'.")
    else:
        print("⚠️ No data extracted.")

if __name__ == "__main__":
    main()
