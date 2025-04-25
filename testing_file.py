import requests
import warnings
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np
import re  # Import the regular expression module

warnings.filterwarnings("ignore")

def generate_url(case_no):
    """
    Generates the URL for a given case number.

    Args:
        case_no (int): The case number.

    Returns:
        str: The generated URL.
    """
    base_url1 = "http://cdsinfo.cadence.com/cgi-bin/cdsinfoprod?input="
    base_url2 = "&type=_&codmode=p"
    generated_url = base_url1 + str(case_no) + base_url2
    return generated_url

def fetch_url_content(url):
    """
    Fetches the HTML content from a given URL.

    Args:
        url (str): The URL to fetch.

    Returns:
        str: The HTML content, or None on error.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for bad response status
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return None

def extract_description(html_content):
    """
    Extracts the case description from the HTML content.

    Args:
        html_content (str): The HTML content.

    Returns:
        str: The extracted description, or an empty string if not found.
    """
    start = "Description"
    end = "Severity"
    try:
        start_pos = html_content.find(start)
        end_pos = html_content.find(end, start_pos) #start search from start position

        if start_pos != -1 and end_pos != -1:
            description_text = html_content[start_pos + len(start):end_pos].strip()
            soup_desc = BeautifulSoup(description_text, 'html.parser')
            desc = soup_desc.get_text().strip()
            return desc
        elif start_pos != -1:
             description_text = html_content[start_pos + len(start):].strip()
             soup_desc = BeautifulSoup(description_text, 'html.parser')
             desc = soup_desc.get_text().strip()
             return desc
        else:
            return ""
    except Exception as e:
        print(f"Error extracting description: {e}")
        return ""

def extract_case_info(html_content):
    """Extracts case number, title, and environment.

    Args:
        html_content (str): The HTML content to parse.

    Returns:
        tuple: (case_number, case_title, environment) or (None, None, None) on error.
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        # Locate the table containing case information.  Adjust the selector if needed.
        info_table = soup.find('table', class_='collection-as-table1')
        if not info_table:
            print("Case info table not found.")
            return None, None, None

        # Extract case number and title
        case_number_elem = info_table.find('font', size='3')
        if case_number_elem:
            case_number_text = case_number_elem.get_text(strip=True)
            case_number = case_number_text.split()[0]  # Extract only the number
            case_title = case_number_text.split('\n')[1]
        else:
            case_number = None
            case_title = None

        # Extract environment
        environment_elem = info_table.find('font', size='2.5')
        environment = environment_elem.find_next('b').get_text(strip=True) if environment_elem else None

        return case_number, case_title, environment

    except Exception as e:
        print(f"Error extracting case info: {e}")
        return None, None, None
    
def extract_product_info(html_content):
    """
    Extracts product information from the HTML content.

    Args:
        html_content (str): The HTML content.

    Returns:
        dict: A dictionary containing product information, or an empty dictionary on error.
    """
    product_info = {}
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        product_table = soup.find('table', class_='collection-as-table')
        if product_table:
            rows = product_table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) == 4:
                    label_cell = cols[0].find('span')
                    value_cell1 = cols[1].find('span')
                    label_cell2 = cols[2].find('span')
                    value_cell2 = cols[3].find('span')

                    if label_cell and value_cell1:
                        label = label_cell.get_text(strip=True).replace(":", "")
                        value = value_cell1.get_text(strip=True)
                        product_info[label] = value
                    if label_cell2 and value_cell2:
                        label2 = label_cell2.get_text(strip=True).replace(":", "")
                        value2 = value_cell2.get_text(strip=True)
                        product_info[label2] = value2
                elif len(cols) == 2:
                    label_cell = cols[0].find('span')
                    value_cell1 = cols[1].find('span')
                    if label_cell and value_cell1:
                        label = label_cell.get_text(strip=True).replace(":", "")
                        value = value_cell1.get_text(strip=True)
                        product_info[label] = value
        return product_info
    except Exception as e:
        print(f"Error extracting product info: {e}")
        return {}
    
def extract_summary(html_content):
    """
    Extracts the case summary from the HTML content.

    Args:
        html_content (str): The HTML content.

    Returns:
        str: The extracted summary, or an empty string if not found.
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        summary_header = soup.find('h4', string='Summary')  # Find the <h4> with "Summary"
        if summary_header:
            summary_table = summary_header.find_next('table', class_='collection-as-table')
            if summary_table:
                summary_text = ""
                for row in summary_table.find_all('tr'):
                    for cell in row.find_all('td'):
                        summary_text += cell.get_text(strip=True) + " "
                return summary_text.strip()
        return ""
    except Exception as e:
        print(f"Error extracting summary: {e}")
        return ""
    
def extract_emails(html_content):
    """
    Extracts email information from the HTML content, handling duplicate tables.

    Args:
        html_content (str): The HTML content.

    Returns:
        list: A list of dictionaries, where each dictionary represents an email.
              Returns an empty list on error or if no emails are found.
    """
    emails = []
    seen_tables = set()  # To track processed tables
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        email_header = soup.find('h4', string='Emails') # Find the emails header
        if email_header:
            table_start = email_header.find_next('table') # Find the table after the emails header
        else:
            return []
        
        if table_start: # Check if the table exists
            tables = [table_start] # create a list
        else:
            return []

        for table in tables:
            # Convert the table to a string representation to check for duplicates
            table_str = str(table)
            if table_str in seen_tables:
                continue  # Skip if this table has already been processed
            seen_tables.add(table_str)  # Add the table string to the set
            
            rows = table.find_all('tr')
            # Extract headers from the first row
            header_row = rows[0]
            headers = [th.text.strip() for th in header_row.find_all('th')]
            
            #check if the headers list is empty or not.
            if not headers:
                continue
            
            # Extract data rows, skipping the header row
            for row in rows[1:]:
                cols = row.find_all('td')
                if len(cols) == len(headers):
                    email_data = {}
                    for i, header in enumerate(headers):
                        cell_text = cols[i].text.strip()
                        email_data[header] = cell_text
                    emails.append(email_data)
        return emails
    except Exception as e:
        print(f"Error extracting emails: {e}")
        return []

def extract_case_number(html_content):
    """
    Extracts the case number from the HTML content.

    Args:
        html_content (str): The HTML content.

    Returns:
        str: The extracted case number, or None if not found.
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        #refined logic to find the case number
        anchor = soup.find('a', href=re.compile(r'case_solution_associate_prd\.pl'))
        if anchor:
            case_number_text = anchor.find_next('font', size='3')
            if case_number_text:
                case_number = case_number_text.get_text(strip=True).split()[0]
                return case_number
        return None
    except Exception as e:
        print(f"Error extracting case number: {e}")
        return None

def main():
    """
    Main function to orchestrate the scraping and CSV generation.
    """
    case_numbers = [46816635]  # Add more case numbers as needed
    all_data = []

    for case_no in case_numbers:
        url = generate_url(case_no)
        html_content = fetch_url_content(url)
        if html_content:
            # Extract data
            case_number, case_title, environment = extract_case_info(html_content) # Extract case title and environment
            description = extract_description(html_content)
            product_info = extract_product_info(html_content)
            summary = extract_summary(html_content)
            emails = extract_emails(html_content)
            case_number_only = extract_case_number(html_content) # Extract case number
            
            # Structure the data, handling missing fields
            case_data = {
                "Case Number": case_number_only,  # Use the extracted case number
                "Case Title": case_title,
                "Case Description": description,
                "Environment": environment,
                "Product Class": product_info.get("Product Class", ""),
                "Product Feature Level 1": product_info.get("Product Feature Level 1", ""),
                "Product Feature": product_info.get("Product Feature", ""),
                "Product Feature Level 2": product_info.get("Product Feature Level 2", ""),
                "Product Feature Version": product_info.get("Product Feature Version", ""),
                "Case Summary": summary,
            }

            # Add email data, flattening it into the main dictionary
            for i, email in enumerate(emails):
                for key, value in email.items():
                    case_data[f"Email {i+1} {key}"] = value  # Flatten email data

            all_data.append(case_data)
        else:
            print(f"Failed to retrieve data for case number: {case_no}")

    # Convert to DataFrame and save to CSV
    if all_data:
        df = pd.DataFrame(all_data)
        # Ensure no duplicate column names
        df = df.loc[:, ~df.columns.duplicated(keep='first')]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file_name = f"case_data_{timestamp}.csv"
        df.to_csv(csv_file_name, index=False, encoding='utf-8')
        print(f"Data successfully written to {csv_file_name}")
    else:
        print("No data to write to CSV.")

if __name__ == "__main__":
    main()
