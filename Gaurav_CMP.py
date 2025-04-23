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
    generated_url = base_url1 + str(case_no) + base_url2
    return generated_url

def fetch_url_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for bad response status
        return response.text
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return None
    
def extract_description(html_content):
  
    start = "Description"
    position = html_content.find(start)
    if position != -1:
        new_content = html_content[position:]
    end = "Severity"
    position = new_content.find(end)
    if position != -1:
        new_content2 = new_content[:position]
    soup_desc = BeautifulSoup(new_content2, 'html.parser')
    desc = soup_desc.get_text()[12:]
    return desc

def header_extractor(html_content):
    try:
        
        start_marker = "Emails"
        end_marker = "Open Activities"
            
        start_pos = html_content.find(start_marker)
        end_pos = html_content.find(end_marker)

        if start_pos == -1 or end_pos == -1:
            
            return "Start or end marker not found in the HTML content"

        new_content = html_content[start_pos:end_pos]
        soup = BeautifulSoup(new_content, 'html.parser')
        rows = soup.find_all('tr')
        specific_widths = ["10%", "10%", "40%", "30%", "10%"]
        headers = []
        for row in rows:
            tds = row.find_all('td')
            if len(tds) == 5:
                widths = []
                for td in tds:
                    style = td.get('style', '')
                    width = ''
                    for part in style.split(';'):
                        if 'width:' in part:
                            width = part.split('width:')[1].strip()
                    widths.append(width)
                
                if widths == specific_widths:
                    td_texts = [td.get_text(strip=True) for td in tds]
                    headers.append(td_texts)
        
        return headers

    except Exception as e:
        print(f"Exception in cleanup_emails for case {case_no}: {e}")
        return f"Exception in cleanup_emails for case {case_no}: {e}"
    
def cleanup_emails(html_content):
    seen_hashes = set()  # Track hashes of processed content
    try:
        
        start_marker = "Emails"
        end_marker = "Open Activities"
            
        start_pos = html_content.find(start_marker)
        end_pos = html_content.find(end_marker)

        if start_pos == -1 or end_pos == -1:
            
            return "Start or end marker not found in the HTML content"

        new_content = html_content[start_pos:end_pos]
        soup = BeautifulSoup(new_content, 'html.parser')

        #target_elements = soup.find_all('td', attrs={'width': '65.5%'})
        target_elements = soup.find_all('td', attrs={'width': '62.5%', 'colspan':'2'})
        
        cleaned_emails = []
        for elem in target_elements:
            soup2 = BeautifulSoup(str(elem), 'html.parser')
            target_div = soup2.find('div')
            content = str(target_div)
            content_hash = hash(content.strip())  # Generate hash
            
            if content_hash not in seen_hashes:
                seen_hashes.add(content_hash)
                cleaned_emails.append(content)
        return cleaned_emails

    except Exception as e:
        print(f"Exception in cleanup_emails for case {case_no}: {e}")
        return f"Exception in cleanup_emails for case {case_no}: {e}"

def extract_comments(html_content):

    start = "Case Feed"
    position = html_content.find(start)
    if position != -1:
        new_content = html_content[position:]

    end = "Related Articles"
    position = new_content.find(end)
    if position != -1:
        new_content2 = new_content[:position]
    soup = BeautifulSoup(new_content2, 'html.parser')
    target_table = soup.find('table')
    element_comment = []


    if target_table is not None:
        target_tr = target_table.find_all('tr')


        for i in range(1, len(target_tr)):
            element = target_tr[i].find_all('td')
            element_comment.append(element[0].get_text())
            element_comment.append(element[1].get_text())
            element_comment.append(element[2].get_text())
    return element_comment

  


def get_jira_comments(html_content):
    
    start = "Case Comments"
    position = html_content.find(start)
    if position != -1:
        new_content = html_content[position:]
    end = "Case Feed"
    position = new_content.find(end)
    if position != -1:
        new_content2 = new_content[:position]
    soup = BeautifulSoup(new_content2, 'html.parser')
    target_table = soup.find('table')
    jira_comments = []
    if target_table is None:
        return jira_comments
    else:
        target_tr = target_table.find_all('tr')
        for i in range(1, len(target_tr)):
            jira_comments.append(target_tr[i].get_text())

    return jira_comments
 

def check_ccr(html_content):
    
    start="Bug/Enh CCR"
    position=html_content.find(start)
    if position!=-1:
        new_content=html_content[position:]
    soup=BeautifulSoup(new_content,'html.parser')
    element=soup.find('td')
   
    if len((element.get_text()).strip())!=0:
        ccr_no=(element.get_text()).strip()
    else:
        ccr_no="No ccr."
 
    return ccr_no


def case_title(html_content):
    start = "Would you like to associate an Article to this Case"
    position = html_content.find(start)
    if position != -1:
        new_content = html_content[position+51:]
    end = "Environment"
    position = new_content.find(end)
    if position != -1:
        new_content2 = new_content[:position]
        soup = BeautifulSoup(new_content2, 'html.parser')
        text = soup.get_text(strip=True)[19:]

    
    return(text)


#following functions are only printed if ccr_no exists
def extract_ccr_desc(url):
    response=requests.get(url)
    html_content=response.text
    start = "DESCRIPTION"
    position = html_content.find(start)
    if position != -1:
        new_content = html_content[position:]

    end = "NOTES"
    position = new_content.find(end)
    if position != -1:
        new_content2 = new_content[:position]
    soup_desc = BeautifulSoup(new_content2, 'html.parser')
    desc = soup_desc.get_text()[12:]
    return desc

def extract_notes(url):
    response=requests.get(url)
    html_content=response.text
    start = "NOTES"
    position = html_content.find(start)
    if position != -1:
        new_content = html_content[position:]

    end = "AUDIT TRAIL" #change this till audit trail
    position = new_content.find(end)
    if position != -1:
        new_content2 = new_content[:position]
    soup_notes = BeautifulSoup(new_content2, 'html.parser')
    notes = soup_notes.get_text()
    indices = []
    start = 0
    while True:
        index = notes.find("Appended by:", start)
        if index == -1:
            break
        indices.append(index)
        start = index + 1
    L = []
    for i in range(len(indices) - 1):
        start_idx = indices[i]
        end_idx = indices[i + 1]
        s = notes[start_idx:end_idx]
        L.append(s.strip())
    if indices:
        start_idx = indices[-1]
        s = notes[start_idx:]
        L.append(s.strip())
    return L


def parse_date(date_str):
    formats = ['%m/%d/%Y, %H:%M:%S','%d/%m/%Y, %H:%M:%S', '%d/%m/%Y %H:%M', '%Y/%d/%m %H:%M', '%Y-%m-%d %H:%M:%S',
               '%A, %d %B %Y at %I:%M %p', '%A, %d %B %Y at %H:%M', '%d %B %Y %H:%M:%S']
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            pass
    return None
#just these 2


def gen_string(case_no):
    try:
        generated_url = generate_url(case_no)
        html_content = fetch_url_content(generated_url)
        
        if not html_content:
            return "Error fetching content from URL."

        case_title1 = case_title(html_content)
        
        case_thread = """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Rubik:ital,wght@0,300..900;1,300..900&display=swap');
        
        body {
          font-family: 'Rubik', sans-serif;
          
        }
        
        #customers {
          font-family: 'Rubik', sans-serif;
          border-collapse: collapse;
          width: 100%;
        }
        
        #customers td, #customers th {
          border: 1px solid #ddd;
          padding: 8px;
          font-size : 14px;
        }
        
        #customers tr:nth-child(even) {
          background-color: #f2f2f2;
        }
        
        #customers tr:hover {
          background-color: #ddd;
        }
        
        #customers th {
          padding-top: 12px;
          padding-bottom: 12px;
          text-align: left;
          background-color:#0C72C7;
          color: white;
        }
        .section {
          border: 1px solid #ddd;
          padding: 15px;
          margin-bottom: 20px;
          border-radius: 5px;
        }
        
        .section h2 {
          font-size: 20px;
          color: #333;
          border-bottom: 2px solid #0C72C7;
          padding-bottom: 5px;
        }
        
        .section p {
          margin: 10px 0;
        }
        
        .section b {
          color: #0C72C7;
        }
        
        .description {
          border-left: 4px solid #0C72C7;
          padding-left: 15px;
          background-color: #f9f9f9;
        }
        </style>
        """
        desc = extract_description(html_content)
        case_thread += f"""
            <h3>Case Information:</h3>
            <div class="section">
                
                <p><b>Case Number:</b> <a href="http://cdsinfo.cadence.com/cgi-bin/cdsinfoprod?input={case_no}&type=_&codmode=p" target = "_blank">{case_no}</a></p>
                <p><b>Case Title:</b></p>
                <div class="description">{case_title1}</div>
                <p><b>Case Decription:</b></p>
                <div class="description">{desc}</div>
            </div>
            
            """

        paired_with_dates = []

        headers = header_extractor(html_content)
        emails = cleanup_emails(html_content)
        #print(emails)

        ccr_no = check_ccr(html_content)
        
        if ccr_no != "No ccr.":
            ccr_url = generate_url(int(ccr_no))
            ccr_desc = extract_ccr_desc(ccr_url)
            case_thread += f"""
            <div class="section">
                <h2>CCR Information</h2>
                <p><b>CCR Number:</b> <a href="http://cdsinfo.cadence.com/cgi-bin/cdsinfoprod?input={ccr_no}&type=_&codmode=p" target = "_blank">{ccr_no}</a></p>
                <p><b>CCR Description:</b></p>
                <div class="description">{ccr_desc}</div>
            </div>
            """
            L = extract_notes(ccr_url)
            for i in range(len(L)):
                start = L[i].find("On:")
                end = L[i].find("====")
                d = L[i][start+4:end-1]
                start2 = L[i].find("by:")
                end2 = L[i].find("On:")
                sender = L[i][start2+4:end2-1]
                d_obj = parse_date(d)
                if d_obj:
                    paired_with_dates.append([d_obj, "CCR-NOTE", sender, "-", d, L[i]])
        
        

        emails = iter(emails)
        for header in headers[1:]:
            date = header[4]
            d_obj = parse_date(date)
            email_body = next(emails, None)
            paired_with_dates.append([d_obj, header[0], header[3], header[2], header[4], email_body])
        
        element_comment = extract_comments(html_content)
        comments = [element_comment[i:i + 3] for i in range(0, len(element_comment), 3)]
        for comment in comments:
            try:
                d_obj = parse_date(comment[2])
                if d_obj:
                    paired_with_dates.append([d_obj, "COMMENT", comment[1], "-", comment[2], comment[0]])
            except IndexError:
                pass

        jira_comments = get_jira_comments(html_content)
        if len(jira_comments) == 0:
            jira_comments.append("No jira comments.")
        
        if jira_comments[0] != "No jira comments.":
            for i in range(len(jira_comments)):
                k = jira_comments[i].find("(")
                d = jira_comments[i][k+1:k+21]
                s = jira_comments[i].find("Created By:")
                sender = jira_comments[i][s+12:s+31]
                d_obj = parse_date(d)
                paired_with_dates.append([d_obj, "Jira comment", sender, "-", d, jira_comments[i]])

        sorted_paired_with_dates = sorted(paired_with_dates, key=lambda x: x[0])
        case_thread+=f"<b>COMMUNICATIONS:</b><br><br>"
        html_table = "<table id='customers'>"
        html_table += "<tr><th>TYPE</th><th>SENDER</th><th>SUBJECT</th><th>DATE</th><th>NO. OF DAYS</th></tr>"
        for i in range(len(sorted_paired_with_dates)):
            if i != 0:
                start = sorted_paired_with_dates[i-1][0]
                end = sorted_paired_with_dates[i][0]
                diff = np.busday_count(start.date(), end.date())
                html_table += f"<tr><td>{sorted_paired_with_dates[i][1]}</td><td>{sorted_paired_with_dates[i][2]}</td><td>{sorted_paired_with_dates[i][3]}</td><td>{sorted_paired_with_dates[i][4]}</td><td>{diff}</td><tr border='0'><td colspan='5'>{sorted_paired_with_dates[i][5]}</td></tr></tr>"
            else:
                html_table += f"<tr><td>{sorted_paired_with_dates[i][1]}</td><td>{sorted_paired_with_dates[i][2]}</td><td>{sorted_paired_with_dates[i][3]}</td><td>{sorted_paired_with_dates[i][4]}</td><td>Initial Mail</td><tr border='0'><td colspan='5'>{sorted_paired_with_dates[i][5]}</td></tr></tr>"
        
        html_table += "</table>"
        case_thread += html_table

        return case_thread

    except Exception as e:
        print(f"Exception in gen_string for case {case_no}: {e}")
        return f"Exception in gen_string for case {case_no}: {e}"


def index1(environ):
    body = environ['QUERY_STRING']

    try:
        if body:
            args = body.replace("%20", " ")
            args = args.replace("%27", "'")
            for arg in args.split("&"):
                key = arg.split("=")[0]
                val = arg.split("=")[1]
                case_thread = gen_string(val)
                return case_thread
    except Exception as e:
        print('Exception:' + str(e))
        return "Error processing request."

    return ""

def application(environ, start_response):
    status = '200 OK'
    output = index1(environ).encode('utf-8')

    response_headers = [('Content-type', 'text/html'),
                        ('Content-Length', str(len(output))),
                        ('Cache-Control', 'no-cache, no-store, must-revalidate'),
                        ('Pragma', 'no-cache'),
                        ('Expires', '0')]

    start_response(status, response_headers)

    return [output]


def index_local(id):
    try:
        case_thread = gen_string(id)
        soup = BeautifulSoup(case_thread, 'html.parser')
        
        # Remove duplicate sections (if any)
        sections = set()
        cleaned_content = []
        for element in soup.stripped_strings:
            if element not in sections:
                cleaned_content.append(element)
                sections.add(element)
        
        # Format text with line breaks and spacing
        structured_text = []
        for line in cleaned_content:
            if line.startswith(('Case Information:', 'CCR Information', 'COMMUNICATIONS:')):
                structured_text.append(f"\n\n### {line} ###\n")
            elif ':' in line:
                structured_text.append(f"  - {line}")
            else:
                structured_text.append(f"    {line}")
        
        return '\n'.join(structured_text)
    
    except Exception as e:
        return f"Error: {str(e)}"
    
a= index_local('46816635')
print(a)

soup = BeautifulSoup(a, 'html.parser')

a = soup.get_text() # Extract text content without HTML tags
# a = soup.prettify()


# Save 'a' output to a text file
# with open('output.txt', 'w') as f:
#     f.write(a)


# Create a DataFrame with the HTML content
df = pd.DataFrame([a], columns=["HTML_Content"])

# Save the DataFrame to a text file
df.to_csv('output.txt', index=False, header=False)
