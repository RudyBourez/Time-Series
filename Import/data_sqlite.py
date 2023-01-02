from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Date
import pandas as pd
import requests
import datetime

#Initialisation de la base de donnée
engine = create_engine("sqlite:///../found_objects.db")
Base = declarative_base()

class Objects(Base):
    __tablename__ = 'Objects'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date)
    restitution_date = Column(Date)
    gare = Column(String)
    nature = Column(String)
    type = Column(String)
    nom = Column(String)

class Meteo(Base):
    __tablename__ = 'Meteo'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date)
    restitution_date = Column(Date)
    gare = Column(String)
    nature = Column(String)
    type = Column(String)
    nom = Column(String)
    
class Train(Base):
    __tablename__ = 'Train'
    id = Column(Integer, primary_key=True, autoincrement=True)
    
Base.metadata.create_all(engine)

# Initialisation des dataframe
dataframe_objects = pd.DataFrame()
dataframe_meteo = pd.DataFrame()
dataframe_train = pd.DataFrame()


# Récupération des données
for i in range ((datetime.date.today() - datetime.timedelta(days=2555)).year, datetime.date.today().year+1):
    data_objects = requests.get(f'https://ressources.data.sncf.com/api/records/1.0/search/?dataset=objets-trouves-restitution&q=&rows=10000&sort=date&facet=date&facet=gc_obo_date_heure_restitution_c&facet=gc_obo_gare_origine_r_name&facet=gc_obo_nature_c&facet=gc_obo_type_c&facet=gc_obo_nom_recordtype_sc_c&refine.gc_obo_gare_origine_r_name=Lille+Europe&refine.date={i}').json()
    data_meteo = requests.get(f'https://public.opendatasoft.com/api/records/1.0/search/?dataset=donnees-synop-essentielles-omm&q=&rows=10000&sort=date&facet=date&facet=nom&facet=temps_present&facet=libgeo&facet=nom_epci&facet=nom_dept&facet=nom_reg&refine.date={i}').json()
    data_train = requests.get(f'https://ressources.data.sncf.com/api/records/1.0/search/?dataset=regularite-mensuelle-tgv-aqst&q=&rows=10000&sort=date&facet=date&facet=service&facet=gare_depart&facet=gare_arrivee&refine.gare_arrivee=LILLE&refine.date={i}').json()
        
    df_objects = pd.DataFrame(data_objects['records'])
    df_objects = pd.DataFrame(list(df_objects['fields']))
    df_meteo = pd.DataFrame(data_meteo['records'])
    df_meteo = pd.DataFrame(list(df_meteo['fields']))
    try:
        df_train = pd.DataFrame(data_train['records'])
        df_train = pd.DataFrame(list(df_train['fields']))
        dataframe_train = pd.concat([dataframe_train, df_train], ignore_index=True)
    except:
        pass
    dataframe_objects = pd.concat([dataframe_objects, df_objects], ignore_index=True)
    dataframe_meteo = pd.concat([dataframe_meteo, df_meteo], ignore_index=True)
        
dataframe_objects.rename(columns={'gc_obo_gare_origine_r_code_uic_c': "code",
                        "gc_obo_date_heure_restitution_c": "restitution_date",
                        'gc_obo_gare_origine_r_name':"gare",
                        'gc_obo_nature_c':"nature",
                        'gc_obo_type_c':"type",
                        'gc_obo_nom_recordtype_sc_c':"nom"}, inplace=True)

dataframe_objects.drop(["code"], axis=1, inplace=True)
dataframe_meteo = dataframe_meteo.loc[dataframe_meteo["code_dep"]=='59']

# Insertion des données dans la BDD
dataframe_objects.to_csv('objects.csv', index=False)
dataframe_meteo.to_csv('meteo.csv', index=False)
dataframe_train.to_csv('train.csv', index=False)
#dataframe_objects.to_sql(name='Objects', con=engine, if_exists = 'append', index=False)
#dataframe_objects.to_sql(name='Meteo', con=engine, if_exists = 'append', index=False)