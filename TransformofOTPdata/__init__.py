import logging
import pandas as pd
import openpyxl
import xlrd
import azure.functions as func

from azure.storage.blob import ContainerClient, BlobServiceClient

"""
    Get otp time series into a dataframe
"""
def get_on_time_performance_dataframe(): 
    # BLOB SAS URL
    url = "https://s12023.blob.core.windows.net/dataset1/OTP_Time_Series_Master_Current_2.xlsx?sp=r&st=2023-05-04T11:15:51Z&se=2023-05-04T19:15:51Z&spr=https&sv=2022-11-02&sr=b&sig=DSXVIGp3I%2Fz3lhvob8pNEscGH1L7AqCpFRSib4JG72I%3D"
    xls = pd.ExcelFile(url)

    number_of_years = 3
    list_of_dfs = []
    for sheet in xls.sheet_names[0:number_of_years]:
        temp_df = xls.parse(sheet)
        list_of_dfs.append(temp_df)
    dataframe = pd.concat(list_of_dfs, ignore_index=True)

    return dataframe

def aus_iata_dataframe(): 
    url = "https://raw.githubusercontent.com/datasets/airport-codes/master/data/airport-codes.csv"
    iata = pd.read_csv(url)
    aus_iata = iata[iata.iso_country == "AU"]
    # Extract relevant columns
    cols = ['type', 'name', 'iata_code']
    notna_mask = aus_iata['iata_code'].notna()
    aus_iata = aus_iata.loc[notna_mask, cols]
    return aus_iata

"""
    route of the form: port-port
    convert port into iata code
"""
def turn_route_iata(route, iata_code_dict):
    port_list = route.split("-")
    for index in range(0, len(port_list)):
        port = port_list[index]
        port_list[index] = iata_code_dict[port]
    return "-".join(port_list)

def create_iata_code_columns(otp_df):
    aus_iata = aus_iata_dataframe()
    # List of departing ports and list of arriving ports
    unique_departing = otp_df['Departing Port'].unique()
    unique_arriving = otp_df['Arriving Port'].unique()

    # Create a dictionary with port and iata code as the 
    # key value pair
    iata_code_dict = {}
    for port in unique_departing:
        temp = aus_iata[aus_iata['name'].str.contains(port)]
        if port == "All Ports":
            iata_code_dict[port] = "ALL"
        # Port name only shows up in one airport
        elif len(temp.index) == 1:
            iata_code_dict[port] = temp.iloc[0]['iata_code']
        # Port name shows up in multiple locations
        else:
            large_airport = temp[temp['type'].str.contains('large_airport')]
            # Assume a location only has one "large_airport"
            if len(large_airport.index) == 1:
                iata_code_dict[port] = large_airport.iloc[0]['iata_code']
            else:
                medium_airport = temp[temp['type'].str.contains('medium_airport')]
                # Places does not have a large_airport, again assume location
                # only has one "medium_airport"
                if len(medium_airport.index) == 1:
                    iata_code_dict[port] = medium_airport.iloc[0]['iata_code']

    otp_df['IATA Departing Port'] = otp_df['Departing Port'].apply(
        lambda port: port.replace(port, iata_code_dict[port])
    )
    otp_df['IATA Arriving Port'] = otp_df['Arriving Port'].apply(
        lambda port: port.replace(port, iata_code_dict[port])
    )
    otp_df['IATA Route'] = otp_df['Route'].apply(
    lambda route: turn_route_iata(route, iata_code_dict))

def create_year_month_cols(otp_df): 
    otp_df = otp_df.rename(columns = {"Month":"Month-Year"})
    otp_df['Month'] = otp_df['Month-Year'].dt.month
    otp_df['Year'] = otp_df['Month-Year'].dt.year
    return otp_df

"""
    Turn na strings into -1
    otherwise divide the number by 100
"""
def clean_percent_col(val):
    if val == "na":
        return -1
    else:
        return float(val)/100
    
def create_percent_cols(otp_df): 
    otp_df['Cancellations'] = otp_df['Cancellations'].div(100)
    otp_df['OnTime Departures \n(%)'] = otp_df['OnTime Departures \n(%)'].apply(
    clean_percent_col)
    otp_df['OnTime Arrivals \n(%)'] = otp_df['OnTime Arrivals \n(%)'].apply(
        clean_percent_col)
    otp_df['Cancellations \n\n(%)'] = otp_df['Cancellations \n\n(%)'].apply(
        clean_percent_col)
    otp_df['Delayed Arrivals (%)'] = 1 - otp_df['OnTime Arrivals \n(%)']
    otp_df['Delayed Departures (%)'] = 1 - otp_df['OnTime Departures \n(%)']
    otp_df['Delayed Percent Addition'] = otp_df['Delayed Arrivals (%)'] + otp_df['Delayed Departures (%)']


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    otp_df = get_on_time_performance_dataframe()
    otp_df = create_year_month_cols(otp_df)
    create_iata_code_columns(otp_df)
    create_percent_cols(otp_df)
    output = otp_df.to_csv(index=False, encoding='utf-8')

    s12023_connection_string = "DefaultEndpointsProtocol=https;AccountName=s12023;AccountKey=ssWeoEHSGxnP1H+4qYk/K6ipOpMXiUO5B2wNcIIkctS5jkAMUare/g/rw37weQjzdK6BbFgsj7qb+AStg1Ms8Q==;EndpointSuffix=core.windows.net"
    container_client = ContainerClient.from_connection_string(
        conn_str=s12023_connection_string,
        container_name="transformed-data-navigational-database"
    )
    container_client.upload_blob(
        name="Transformed_OTP.csv",
        data=output,
        overwrite=True
    )

    return func.HttpResponse(
             "This HTTP triggered function executed successfully." + 
             " OTP Data has been put into the transformed-data-navigational-database",
             status_code=200
    )
