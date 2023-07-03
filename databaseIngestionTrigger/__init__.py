import os
import mysql.connector
import pandas as pd
from azure.storage.blob import BlobServiceClient

def main(myblob: str):
    # Set up connection to Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])
    blob_client = blob_service_client.get_blob_client(container="transformed-data-navigational-database", blob=myblob)
    
    # Read CSV file from Blob Storage
    csv_data = blob_client.download_blob().content_as_text()
    csv_df = pd.read_csv(StringIO(csv_data))
    
    # Convert data to list of tuples
    data = [tuple(x) for x in csv_df.to_records(index=False)]
    
    # Connect to MySQL database
    conn = mysql.connector.connect(user=os.environ["PaceS12023Admin"], password=os.environ["Matt$1th"],
                               host=os.environ["pace-s1-2023-pub-access.mysql.database.azure.com"], database=os.environ["pacedb"])
    
    # Insert data into MySQL database
    cursor = conn.cursor()
    query = "INSERT INTO mytable (col1, col2, col3, col4, col5 col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.executemany(query, data)
    conn.commit()
    cursor.close()
    conn.close()
