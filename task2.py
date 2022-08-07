import gcloud
import pandas as pd
import gcsfs
from gcloud import storage




def upload_data_from_GDC(path, table):
	
	""" This is a function, where the data is
	fetched from Google Cloud Storage, converted into 
	a Pandas dataframe, manipulated, and then send to Google BigQuery"""


	storage_client = storage.Client()   #intitializing the Google Storage.

	df = pd.read_csv(path, encoding='latin-1') #Reading the dataframe. Encoding has been specified as it was giving errors.
	
	df_updated= df[(df['Gender']== 'Male') & (df['Medal'].notnull())]  #Data is manipulated
	
	
	table_id = table  #Specifying the table id of BigQuery.

	df_updated.to_gbq(table_id,if_exists ='replace', chunksize=100 )  #Making use of the Pandas fucntion to send data to BigQuery.

	return df_updated  #Return the manipulated dataframe.





## Now making use of the function defined.
data= upload_data_from_GDC(path= "gs://{your cloud storage path}/file.csv", table="{your table id in BigQuery}" )

