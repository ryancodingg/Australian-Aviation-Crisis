import logging
import pandas as pd
import azure.functions as func

from azure.storage.blob import ContainerClient
def get_airport_passenger_dataframe(): 
    # BLOB SAS URL 
    url = "https://s12023.blob.core.windows.net/dataset1/WebMonthlyAirportDecember2022.xlsx?sp=r&st=2023-05-04T13:18:51Z&se=2023-05-04T21:18:51Z&spr=https&sv=2022-11-02&sr=b&sig=7%2F7XsqepcGaDub2Ist8aEj8n4Z7%2BK8hQFJTjL0yiFLs%3D"
    pass_df = pd.read_excel(url, sheet_name = "Airport Passengers", header = 6)
    return pass_df

def get_aircraft_movement_dataframe(): 
    url = "https://s12023.blob.core.windows.net/dataset1/WebMonthlyAirportDecember2022.xlsx?sp=r&st=2023-05-04T13:18:51Z&se=2023-05-04T21:18:51Z&spr=https&sv=2022-11-02&sr=b&sig=7%2F7XsqepcGaDub2Ist8aEj8n4Z7%2BK8hQFJTjL0yiFLs%3D"
    move_df = pd.read_excel(url, sheet_name = "Aircraft Movements", header = 5)
    return move_df

def rename_cols(df, suffix): 
    col_name_list = df.columns.tolist()
    col_rename_dict = rename_dict(col_name_list, suffix)
    rename_df = df.rename(columns = col_rename_dict)
    return rename_df

def rename_dict(col_list, sheet_name):
    col_replace_dict = {}
    prefix = ""
    domestic = [3, 4, 5]
    internat = [6, 7, 8]
    for index in range(3, len(col_list)):
        if index in domestic:
            prefix = "Domestic"
        elif index in internat:
            prefix = "International"
        else:
            prefix = "Total"
        clean_name = col_list[index].split(".")[0].capitalize()
        value = ""
        if index != len(col_list) - 1:
            value = prefix + clean_name + sheet_name
        else:
            value = "Total" + sheet_name
        col_replace_dict[col_list[index]] = value
    return col_replace_dict

# def join_df(df1, df2): 
#     df1_rows = len(df1.index)
#     df2_rows = len(df2.index)
#     df1_cols = df1.columns.tolist()
#     df2_cols = df2.columns.tolist()
#     com_cols = list(set(df1_cols).intersection(df2_cols))
#     result = pd.DataFrame()
#     if df1_rows > df2_rows:
#         result = pd.merge(df1, df2, how="left",
#                        left_on=com_cols,
#                        right_on=com_cols)
#     else: 
#         result = pd.merge(df2, df1, how ="left",
#                           left_on=com_cols,
#                           right_on=com_cols
#                           )
#     return result

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    pass_df = get_airport_passenger_dataframe()
    move_df = get_airport_passenger_dataframe()
    pass_df = rename_cols(pass_df, "Passengers")
    move_df = rename_cols(move_df, "AircraftMovement")

    move_df['AIRPORT'] = move_df["AIRPORT"].str.capitalize()
    pass_df['AIRPORT'] = pass_df["AIRPORT"].str.capitalize()

    move_rows = len(move_df.index)
    pass_rows = len(pass_df.index)
    join_df = pd.DataFrame()
    com_cols = ["AIRPORT", "Year", "Month"]
    if move_rows > pass_rows:
        join_df = pd.merge(move_df, pass_df, how="left",
                       left_on=com_cols,
                       right_on=com_cols)
    else:
        join_df = pd.merge(pass_df, move_df, how="left",
                       left_on=com_cols,
                       right_on=com_cols)
    # join_df = join_df(pass_df, move_df)

    # output = pass_df.to_csv(index=False, encoding='utf-8')
    output = join_df.to_csv(index=False, encoding='utf-8')

    s12023_connection_string = "DefaultEndpointsProtocol=https;AccountName=s12023;AccountKey=ssWeoEHSGxnP1H+4qYk/K6ipOpMXiUO5B2wNcIIkctS5jkAMUare/g/rw37weQjzdK6BbFgsj7qb+AStg1Ms8Q==;EndpointSuffix=core.windows.net"
    container_client = ContainerClient.from_connection_string(
        conn_str=s12023_connection_string,
        container_name="transformed-data-navigational-database"
    )
    container_client.upload_blob(
        name="AirportStats.csv",
        data=output,
        overwrite=True
    )
    
    return func.HttpResponse(
             "This HTTP triggered function executed successfully. AirportStat.csv is in transformed-data-navigational-database",
             status_code=200
        )

