from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Date
import pandas as pd
import requests

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
    
Base.metadata.create_all(engine)

dataframe_objects = pd.DataFrame(columns=['gc_obo_gare_origine_r_code_uic_c', 'gc_obo_type_c',
       'gc_obo_gare_origine_r_name', 'gc_obo_nature_c',
       'gc_obo_nom_recordtype_sc_c', 'date',
       'gc_obo_date_heure_restitution_c'])

dataframe_meteo = pd.DataFrame(columns=[
        'hbas', 'rafper', 'code_dep', 'libgeo','pmer', 'code_epci', 't', 'ww',
        'raf10', 'numer_sta', 'hnuage1', 'code_reg', 'codegeo', 'rr3',
        'coordonnees', 'rr12', 'tc', 'rr6', 'rr24', 'per', 'nom_dept',
        'mois_de_l_annee', 'nom_reg', 'td', 'longitude', 'temps_present', 'ff',
        'tend24', 'altitude', 'dd', 'nom_epci', 'rr1', 'date', 'nom', 'u',
        'type_de_tendance_barometrique', 'cod_tend', 'nbas', 'tend', 'latitude',
        'pres', 'nnuage1', 'vv', 'tminsolc', 'ht_neige', 'etat_sol', 'tminsol',
        'n', 'tx24', 'tx24c', 'tn24', 'tn24c', 'ctype1', 'w1', 'ch', 'cm',
        'temps_passe_1', 'cl', 'w2', 'ssfrai', 'perssfrai', 'nnuage3',
        'hnuage2', 'nnuage2', 'hnuage3', 'ctype2', 'geop', 'tn12c', 'tx12',
        'tn12', 'tx12c', 'ctype3', 'ctype4', 'hnuage4', 'nnuage4'
        ])

dataframe_train = pd.DataFrame(columns=[
       'hbas', 'rafper', 'code_dep', 'libgeo', 'pmer', 'code_epci', 't', 'ww',
       'raf10', 'numer_sta', 'hnuage1', 'code_reg', 'codegeo', 'rr3',
       'coordonnees', 'rr12', 'tc', 'rr6', 'rr24', 'per', 'nom_dept',
       'mois_de_l_annee', 'nom_reg', 'td', 'longitude', 'temps_present', 'ff',
       'tend24', 'altitude', 'dd', 'nom_epci', 'rr1', 'date', 'nom', 'u',
       'type_de_tendance_barometrique', 'cod_tend', 'nbas', 'tend', 'latitude',
       'pres', 'nnuage1', 'vv', 'tminsolc', 'ht_neige', 'etat_sol', 'tminsol',
       'n', 'tx24', 'tx24c', 'tn24', 'tn24c', 'ctype1', 'w1', 'ch', 'cm',
       'temps_passe_1', 'cl', 'w2', 'ssfrai', 'perssfrai', 'nnuage3',
       'hnuage2', 'nnuage2', 'hnuage3', 'ctype2', 'geop', 'tn12c', 'tx12',
       'tn12', 'tx12c', 'ctype3', 'ctype4', 'hnuage4', 'nnuage4',
       'prct_cause_infra', 'retard_moyen_trains_retard_sup15',
       'retard_moyen_tous_trains_arrivee', 'prct_cause_externe',
       'nb_train_prevu', 'prct_cause_gestion_trafic',
       'nb_train_retard_arrivee', 'nb_train_retard_sup_15', 'nb_annulation',
       'prct_cause_prise_en_charge_voyageurs', 'gare_arrivee',
       'retard_moyen_arrivee', 'nb_train_retard_sup_30',
       'nb_train_retard_sup_60', 'service', 'nb_train_depart_retard',
       'commentaires_retard_arrivee', 'prct_cause_gestion_gare', 'gare_depart',
       'duree_moyenne', 'retard_moyen_depart',
       'retard_moyen_tous_trains_depart', 'prct_cause_materiel_roulant'
        ])

for i in range (2016, 2024):
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
#dataframe_objects.to_sql(name='Objects', con=engine, if_exists = 'append', index=False)
#dataframe_objects.to_sql(name='Meteo', con=engine, if_exists = 'append', index=False)