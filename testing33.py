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

def hash_content(fields):
    combined = ' '.join(fields)
    return hashlib.md5(combined.encode('utf-8')).hexdigest()

def parse_case(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    parsed_data = []
    seen_hashes = set()

    # Extract Case Info
    case_number_tag = soup.find('font', string=lambda x: x and 'Case Number' in x)
    case_number = case_number_tag.find_next('font').text.strip() if case_number_tag else ''

    case_title_tag = case_number_tag.find_next('b') if case_number_tag else None
    case_title = case_title_tag.text.strip() if case_title_tag else ''

    # Summary
    summary_section = soup.find('h4', string='Summary')
    case_summary = ''
    if summary_section:
        summary_td = summary_section.find_next('td', {'colspan': '3'})
        if summary_td:
            case_summary = summary_td.get_text(separator=' ', strip=True)

    # Emails Section
    emails_section = soup.find('b', string='Emails')
    if emails_section:
        email_table = emails_section.find_next('table')
        if email_table:
            email_rows = email_table.find_all('tr')
            for row in email_rows:
                cols = row.find_all('td')
                if len(cols) == 5:
                    email_name = cols[0].text.strip()
                    email_status = cols[1].text.strip()
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

                    body_hash = hash_content([email_name, email_status, email_subject, email_from, email_date, email_body])
                    if body_hash not in seen_hashes:
                        seen_hashes.add(body_hash)

                        parsed_data.append({
                            'Case Number': case_number,
                            'Case Title': case_title,
                            'Case Summary': case_summary,
                            'Email Name': email_name,
                            'Email Status': email_status,
                            'Email Subject': email_subject,
                            'Email From': email_from,
                            'Email Date': email_date,
                            'Email Body': email_body,
                            'Case Feed Author': '',
                            'Case Feed Comment': '',
                            'Case Feed Timestamp': ''
                        })

    # Case Feed Section
    case_feed_section = soup.find('h4', string='Case Feed')
    if case_feed_section:
        feed_table = case_feed_section.find_next('table')
        if feed_table:
            rows = feed_table.find_all('tr')[1:]  # skip header row
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 3:
                    feed_author = cols[0].text.strip()
                    feed_comment = cols[1].text.strip()
                    feed_time = cols[2].text.strip()

                    body_hash = hash_content([feed_author, feed_comment, feed_time])
                    if body_hash not in seen_hashes:
                        seen_hashes.add(body_hash)

                        parsed_data.append({
                            'Case Number': case_number,
                            'Case Title': case_title,
                            'Case Summary': case_summary,
                            'Email Name': '',
                            'Email Status': '',
                            'Email Subject': '',
                            'Email From': '',
                            'Email Date': '',
                            'Email Body': '',
                            'Case Feed Author': feed_author,
                            'Case Feed Comment': feed_comment,
                            'Case Feed Timestamp': feed_time
                        })

    return parsed_data

def clean_case_columns(df):
    # Blank out duplicate case number, title, summary rows after first
    for case_number in df['Case Number'].unique():
        indices = df.index[df['Case Number'] == case_number].tolist()
        for idx in indices[1:]:
            df.at[idx, 'Case Number'] = ''
            df.at[idx, 'Case Title'] = ''
            df.at[idx, 'Case Summary'] = ''
    return df

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
        df = clean_case_columns(df)
        df.to_csv('final_cases_output.csv', index=False, encoding='utf-8-sig')
        print("✅ Scraping completed. File saved as 'final_cases_output.csv'.")
    else:
        print("⚠️ No data extracted.")

if __name__ == "__main__":
    main()
