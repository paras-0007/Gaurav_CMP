import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

def extract_section_data(soup, section_title, headers):
    """Extract data from a specific section table"""
    section = soup.find('h4', string=section_title)
    if not section:
        return []
    
    table = section.find_next('table')
    rows = table.find_all('tr')[1:]  # Skip header row
    data = []
    
    for row in rows:
        cells = row.find_all('td')
        if len(cells) >= len(headers):
            row_data = {headers[i]: cell.get_text(strip=True) for i, cell in enumerate(cells)}
            data.append(row_data)
    
    return data

def extract_emails(soup):
    """Extract structured email data with nested details"""
    emails_section = soup.find('b', string='Emails')
    if not emails_section:
        return []
    
    emails_table = emails_section.find_next('table')
    if not emails_table:
        return []
    
    emails = []
    # Skip header and separator rows using more robust selection
    for email_row in emails_table.find_all('tr')[2:]:
        # Skip rows that are just horizontal rules
        if email_row.find('hr'):
            continue
            
        cols = email_row.find_all('td')
        # Check we have exactly 5 columns before processing
        if len(cols) == 5:
            try:
                email_data = {
                    'Email Name': cols[0].get_text(strip=True),
                    'Status': cols[1].get_text(strip=True),
                    'Subject': cols[2].find('i').get_text(strip=True) if cols[2].find('i') else '',
                    'From Address': cols[3].get_text(strip=True),
                    'Message Date': cols[4].get_text(strip=True),
                    'Email Body': cols[2].get_text(strip=True)  # Get full subject text
                }
                emails.append(email_data)
            except (AttributeError, IndexError) as e:
                print(f"Skipping malformed email row: {str(e)}")
                continue
                
    return emails
def parse_html_to_df(html_content):
    """Main parsing function to structure all data"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Base case information
    case_info = {
        'Case Number': soup.find('span', string='Case Number').find_next('span').get_text(strip=True),
        'Subject': soup.find('span', string='Subject').find_next('span').get_text(strip=True),
        'Status': soup.find('span', string='Status').find_next('span').get_text(strip=True),
        'Severity': soup.find('span', string='Severity').find_next('span').get_text(strip=True),
        'Date Opened': soup.find('span', string='Date/Time Opened').find_next('span').get_text(strip=True),
        'Date Closed': soup.find('span', string='Date/Time Closed').find_next('span').get_text(strip=True)
    }

    # Extract structured sections
    product_data = extract_section_data(soup, 'Product Information', 
                                      ['Product Class', 'Product Feature Level 1', 'Product Feature', 'Product Feature Level 2'])
    contact_data = extract_section_data(soup, 'Contact Information', 
                                      ['Contact Name', 'Phone', 'Email', 'Account Name'])
    emails_data = extract_emails(soup)
    
    # Create main DataFrame
    df = pd.DataFrame([case_info])
    
    # Add nested data as JSON columns
    df['Product Information'] = [product_data]
    df['Contact Information'] = [contact_data]
    df['Emails'] = [emails_data]
    
    return df

def html_to_csv(case_number):
    """End-to-end conversion function"""
    url = f"http://cdsinfo.cadence.com/cgi-bin/cdsinfoprod?input={case_number}&type=_&codmode=p"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        df = parse_html_to_df(response.text)
        
        # Explode nested columns
        exploded_dfs = []
        for col in ['Product Information', 'Contact Information', 'Emails']:
            if col in df.columns:
                exploded = pd.json_normalize(df[col]).add_prefix(f'{col}_')
                exploded_dfs.append(exploded)
        
        final_df = pd.concat([df.drop(columns=['Product Information', 'Contact Information', 'Emails'])] + exploded_dfs, axis=1)
        
        # Clean date formats
        date_columns = ['Date Opened', 'Date Closed']
        for col in date_columns:
            final_df[col] = pd.to_datetime(final_df[col], errors='coerce')
        
        final_df.to_csv(f'case_{case_number}_structured.csv', index=False, encoding='utf-8-sig')
        return f"CSV created successfully for case {case_number}"
    
    except Exception as e:
        return f"Error processing case {case_number}: {str(e)}"

# Example usage
print(html_to_csv('46816635'))
