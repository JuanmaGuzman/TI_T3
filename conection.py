from cgitb import text
import gspread 
import json
import io
import pandas as pd
from csv import writer

#Importar a heroku

sa = gspread.service_account(filename="service_account.json")
sh = sa.open("ti_tarea_tres")

wks_1 = sh.worksheet("Hoja 1")
wks_2 = sh.worksheet("Hoja 2")
wks_3 = sh.worksheet("Hoja 3")


def explicit():
    from google.cloud import storage

    # Explicitly use service account credentials by specifying the private key
    # file.
    storage_client = storage.Client.from_service_account_json(
        'taller-integracion-310700-e8a8e3f6c66c.json')

    # The name for the new bucket
    bucket_name = '2022-01-tarea-3'

    df_tur = pd.DataFrame(columns=["Country", "Nights", "Date"])
    df_rec = pd.DataFrame(columns=["Country", "Recycling rate", "Date"])
    df_air = pd.DataFrame(columns=["Country", "Tonnes/ square km", "Date"])

    # Iterate buckets
    blobs = storage_client.list_blobs(bucket_name)
    for blob in blobs:
        if str(blob.name)[-3:] == 'csv':

            fecha_ano = str((blob.name)[8:12]) 
            fecha_mes = str((blob.name)[13:15])
            fecha_dia = '01'
            if '/' in fecha_mes:
                fecha_mes = '0' + fecha_mes[0]

            # Armo fecha del blob
            fecha = fecha_ano + '/' + fecha_mes + '/' + fecha_dia

            # Cargo data del blob
            blob_string = blob.download_as_bytes().decode()
            blob_list = blob_string.split('\r\n')

            for pair in blob_list:
                if pair != "" :
                    items = pair.split(";")
                    country = items[0]
                    nights = items [1]
                    if nights == ":":
                        #print(f"{country}; null")
                        list_al = [country, "null", fecha]
                        df = pd.DataFrame([list_al], columns=["Country", "Nights", "Date"])
                        df_tur = pd.concat([df_tur, df])

                    else:
                        #print(f"{country}; {nights}")
                        list_al = [country, nights, fecha]
                        df = pd.DataFrame([list_al], columns=["Country", "Nights", "Date"])
                        df_tur = pd.concat([df_tur, df])
                else:
                    pass


        if str(blob.name)[-4:] == 'json':
            # Fecha blob
            fecha_ano = str((blob.name)[10:14]) 
            fecha_mes = '01'
            fecha_dia = '01'
            if '/' in fecha_mes:
                fecha_mes = '0' + fecha_mes[0]

            # Armo fecha del blob
            fecha = fecha_ano + '/' + fecha_mes + '/' + fecha_dia

            # Cargo data del blob
            blob_string = blob.download_as_string().decode()
            blob_json = json.loads(blob_string)

            for item in blob_json:
                items_list = list(item.values()) 
                country = items_list[0]
                recyclingrate = items_list[1]
                final_list = [country, recyclingrate, fecha]
                df = pd.DataFrame([final_list], columns=["Country", "Recycling rate", "Date"])
                df_rec = pd.concat([df_rec, df])


        if str(blob.name)[-7:] == 'parquet':
            # Fecha blob
            fecha_ano = str((blob.name)[5:9]) 
            fecha_mes = '01'
            fecha_dia = '01'
            if '/' in fecha_mes:
                fecha_mes = '0' + fecha_mes[0]

            # Armo fecha del blob
            fecha = fecha_ano + '/' + fecha_mes + '/' + fecha_dia
        
            # Cargo data del blob
            step_1 = blob.download_as_string()
            step_2 = io.BytesIO(step_1)
            df = pd.read_parquet(step_2)
            df["Date"] = fecha 

            df_air = pd.concat([df_air, df])



    df_tur = df_tur[["Country", "Nights", "Date"]]  
    wks_1.update([df_tur.columns.values.tolist()] + df_tur.values.tolist())

    df_rec = df_rec[["Country", "Recycling rate", "Date"]]  
    wks_2.update([df_rec.columns.values.tolist()] + df_rec.values.tolist())

    df_air = df_air[["Country", "Tonnes/ square km", "Date"]]
    wks_3.update([df_air.columns.values.tolist()] + df_air.values.tolist())

    print("Finish")

explicit()