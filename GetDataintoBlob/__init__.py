import requests
import logging
import azure.functions as func
import os

from azure.storage.blob import ContainerClient
from bs4 import BeautifulSoup

# Extract relevant csv/xlsx files based on substrings found in the url link
def extract_BITRE_file(url: str, substring: str):
    reqs = requests.get(url)
    soup = BeautifulSoup(reqs.text, "html.parser")
    base_url = "https://www.bitre.gov.au"

    soup_select = soup.select("a[href*=" + substring + "]")
    result_list = []
    for soup_obj in soup_select: 
        result_list.append(base_url + str(soup_obj.get('href')))
    return result_list

# Helper function for get_url_list
def get_relevant_link(url_list):
    for url in url_list: 
        if "1985" in url:
            url_list.remove(url)
    return url_list[0] 

# Get a list containing all the links to all the datasets
# There are extra data sets included in the list
def get_url_list(): 
    url_substr_dict = {}
    url = "https://www.bitre.gov.au/publications/ongoing/general_aviation_activity"
    url_substr_dict[url] = 'xlsx'
    url = "https://www.bitre.gov.au/publications/ongoing/airline_on_time_monthly"
    url_substr_dict[url] = 'Current'
    url = "https://www.bitre.gov.au/statistics/aviation/air_fares"
    url_substr_dict[url] = 'FaresForBI'
    url = "https://www.bitre.gov.au/statistics/aviation/australian_air_distances"
    url_substr_dict[url] = "csv"
    url = "https://www.bitre.gov.au/publications/ongoing/airport_traffic_data"
    url_substr_dict[url] = 'WebMonthly'

    url_list = []
    for url, substr in url_substr_dict.items():
        temp_url_list = extract_BITRE_file(url, substr)
        relevant_url = get_relevant_link(temp_url_list)
        temp_dict = { 
            'url' : relevant_url,
            # url.split('/')[-1] whatever is after the last slash in the url 
            # of the page from where relevant_url is from.
            # relevant_url.split('.')[-1] gives the file extension csv,xslx, ... etc. 
            'filename' : url.split('/')[-1] + '.' + relevant_url.split('.')[-1]
         }
        url_list.append(temp_dict)

    # Jet Fuel link
    url_list.append({
        'url' : "https://www.eia.gov/dnav/pet/hist_xls/EER_EPJK_PF4_RGC_DPGd.xls",
        'filename' : 'Jet_Fuel_Prices.xls'
        })
    #Gasoline link
    url_list.append({
        'url' : "https://www.eia.gov/dnav/pet/hist_xls/EER_EPMRU_PF4_Y35NY_DPGd.xls",
        'filename' : 'Gasoline_Prices.xls'
    })

    # RBA Exchange Rates
    url_list.append({
        'url' : "https://www.rba.gov.au/statistics/tables/xls-hist/2018-2022.xls",
        'filename' : "AUD_USD_Exchange_Rate_2018_2022.xls"
    })
    url_list.append({
        'url' : "https://www.rba.gov.au/statistics/tables/xls-hist/2023-current.xls",
        'filename' : "AUD_USD_Exchange_Rate_2023_curr.xls"
    })

    # Airline Competition Supplementary Data
    url_list.append({
        'url' : "https://www.accc.gov.au/system/files/Supplementary%20Data%20-%20Airlines%20Monitoring%20-%20January%202019%20to%20January%202023.xlsx",
        'filename' : 'Airline_Comp_Supp_Data.xlsx'
    })

    # CASA Dataset
    url_list.append({
        'url' : "https://services.casa.gov.au/CSV/acrftreg.csv",
        'filename' : 'CASA_Data_Aircraft_Register.csv'
    })
    
    return url_list

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    url_list = get_url_list()

    container_client  = ContainerClient.from_connection_string(
        conn_str = os.environ['s12023_storage_key'],
        container_name = "dataset1"
    )

    for url in url_list: 
        response = requests.get(url=url['url'])
        #Check if url is okay
        if response.ok : 
            # Get last element of list, probably file name
            container_client.upload_blob(
                name = url['filename'], 
                data = response.content,
                overwrite = True
            )

    return func.HttpResponse(
             "This HTTP triggered function executed successfully. Data has been put into container dataset1.",
             status_code=200
        )

