import subprocess
import sys

from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os
import requests
import yaml
from tqdm import tqdm

def fetch_data(base_url, year, html_output, download_dir, file_limit):
    subprocess.run(["curl", "-L", "-o", html_output, base_url + str(year)])
    
    with open(html_output, 'r') as file:
        html_content = file.read()

    soup = BeautifulSoup(html_content, 'html.parser')
    csv_links_memory = []

    for row in soup.find_all('tr')[2:]:
        columns = row.find_all('td')
        if columns and columns[2].text.strip().endswith('M'):
            link = urljoin(base_url + str(year) + '/', columns[0].text.strip())
            size_in_mb = float(columns[2].text.strip().replace('M', ''))
            csv_links_memory.append((link, size_in_mb))

    selected_links = [link for link, size in csv_links_memory if size > 45][:file_limit]
    os.makedirs(download_dir, exist_ok=True)

    for link in selected_links:
        response = requests.get(link, stream=True)
        if response.status_code == 200:
            file_path = os.path.join(download_dir, os.path.basename(link))
            total_size = int(response.headers.get('content-length', 0))
            with tqdm(total=total_size, unit='iB', unit_scale=True, desc=os.path.basename(link)) as progress_bar:
                with open(file_path, 'wb') as file:
                    for chunk in response.iter_content(1024):
                        file.write(chunk)
                        progress_bar.update(len(chunk))

def main():
    with open("params.yaml", 'r') as file:
        config = yaml.safe_load(file)

    base_url = config["data_source"]["base_url"]
    year = config["data_source"]["year"]
    html_output = config["data_source"]["output"]
    download_dir = config["data_source"]["temp_dir"]
    file_limit = config["data_source"]["max_files"]

    fetch_data(base_url, year, html_output, download_dir, file_limit)

if __name__ == "__main__":
    main()

