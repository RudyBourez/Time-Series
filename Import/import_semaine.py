# A lancer tous les mardis à 23h

from sqlalchemy import create_engine
import requests
import pandas as pd
import datetime

engine = create_engine("sqlite:///../found_objects.db")

today = datetime.date.today()
last_week = today - datetime.timedelta(days=6)

dataframe = pd.DataFrame(columns=["date"])    
dataframe["date"] = pd.date_range(last_week, today)

for date in dataframe["date"]:
    data_objects = requests.get(f'https://ressources.data.sncf.com/api/records/1.0/search/?dataset=objets-trouves-restitution&q=&rows=9000&sort=date&facet=date&facet=gc_obo_date_heure_restitution_c&facet=gc_obo_gare_origine_r_name&facet=gc_obo_nature_c&facet=gc_obo_type_c&facet=gc_obo_nom_recordtype_sc_c&refine.gc_obo_gare_origine_r_name=Lille+Europe&refine.date=date').json()
    df_objects = pd.DataFrame(data_objects['records'])
    df_objects = pd.DataFrame(list(df_objects['fields']))
    
    dataframe_objects = pd.concat([dataframe_objects, df_objects], ignore_index=True)
    
dataframe_objects.rename(columns={'gc_obo_gare_origine_r_code_uic_c': "code",
                        "gc_obo_date_heure_restitution_c": "restitution_date",
                        'gc_obo_gare_origine_r_name':"gare",
                        'gc_obo_nature_c':"nature",
                        'gc_obo_type_c':"type",
                        'gc_obo_nom_recordtype_sc_c':"nom"}, inplace=True)

dataframe_objects.drop(["code"], axis=1, inplace=True)
dataframe_objects.to_sql(name='Objects', con=engine, if_exists = 'append', index=False)