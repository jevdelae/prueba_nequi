#acceder al archivo de google cloud base eps
try:
  from google.cloud import storage
  import pandas as pd
  import io
  from io import BytesIO
  from datetime import datetime
  from traceback import format_exc
  import numpy as np
  
  #crear cliente conexión Bucket de google
  storage_client = storage.Client.from_service_account_json("/content/drive/MyDrive/Datasets/ingdatosnequi-8799c08e6f65.json")
  bucket_name = 'datasets_ing_datos'
  bucket = storage_client.get_bucket(bucket_name)
  filename = list(bucket.list_blobs(prefix = ''))
  #visualizar elementos del bucket
  for name in filename:
    print(name.name)
  #Seleccionar archivo input/Base_eps_colombia.csv y descargarlo a un dataFrame
  blop = bucket.blob("input/Base_eps_colombia.csv")
  data = blop.download_as_string()
  registros_eps = pd.read_csv(io.BytesIO(data), encoding='utf-8',sep=",")
except:
  with open('/content/drive/MyDrive/Datasets/log_ejecucion.txt', 'a') as f:
    exc = format_exc()
    fecha = str(datetime.now())
    log =  fecha + ';' +'Error en lectura de archivo Base_eps_colombia.csv' + ';' + exc
    f.write(log)
#Renombrar Columnas
try:
  registros_eps = registros_eps.rename(columns={'Genero':'genero',
                                              'Grupo etario ':'grupo_etario',
                                              'Código de la entidad':'cod_entidad',
                                              'Nombre de la entidad':'nombre_entidad',
                                              'Régimen al que pertenece':'regimen',
                                              'Tipo de afiliado':'tipo_afiliado',
                                              'Estado del afiliado':'estado_afiliado',
                                              'Condición del beneficiario':'condicion_benef',
                                              'Zona de Afiliación':'zona_afiliacion',
                                              'Departamento':'departamento',
                                              'Municipio':'municipio',
                                              'Nivel del Sisbén':'nivel_sisben',
                                              'Grupo poblacional del afiliado':'grupo_poblacional',
                                              'cantidad':'cantidad_afiliados'},errors='raise')
except:
  with open('/content/drive/MyDrive/Datasets/log_ejecucion.txt', 'a') as f:
    exc = format_exc()
    fecha = str(datetime.now())
    log =  fecha + ';' +'Error al renombrar las columnas archivo Base_eps_colombia.csv' + ';' + exc
    f.write(log)
cant_reg_elim_eps = len(registros_eps)
#Eliminar registros duplicados
registros_eps = registros_eps.drop_duplicates()
cant_reg_elim_eps = cant_reg_elim_eps - len(registros_eps)
#Homologar grupo etario
registros_eps['grupo_etario'] = registros_eps['grupo_etario'].map({'< 1':'Primera Infancia',
                             '1 a 5':'Primera Infancia',
                             '5 a 15':'Infancia',
                             '15 a 19':'Adolescencia',
                             '19 a 45':'Adultez',
                             '45 a 50':'Adultez',
                             '50 a 55':'Adultez',
                             '55 a 60':'Persona Mayor',
                             '60 a 65':'Persona Mayor',
                             '65 a 70':'Persona Mayor',
                             '70 a 75':'Persona Mayor',
                             '> 75':'Anciano'})
#Se Identifican columnas con registros nulos
base_eps_nulos = registros_eps.isnull().sum()
#Se agrupan los registros de las eps y se almacena local
registros_eps_agrupado = registros_eps.groupby(by = ["departamento","grupo_etario","nombre_entidad","regimen"])['cantidad_afiliados'].sum()

try:
  #Se almacena localmente el resultado
  fecha = datetime.now()
  nom_arc_eps = "eps_agrupado" + str(fecha.year) + str(fecha.month).rjust(2,'0') + str(fecha.day).rjust(2,'0') +".csv"
  registros_eps_agrupado.to_csv('/content/drive/MyDrive/Datasets/'+nom_arc_eps,sep=';')
  #Enviar resultado de casos covid agrupados a la nube de google
  storage_client = storage.Client.from_service_account_json("/content/drive/MyDrive/Datasets/ingdatosnequi-8799c08e6f65.json")
  bucket_name = 'datasets_ing_datos'
  bucket = storage_client.get_bucket(bucket_name)
  filename = "output/" + nom_arc_eps
  blop = bucket.blob(filename)
  blop.upload_from_filename('/content/drive/MyDrive/Datasets/'+nom_arc_eps)
except:
  with open('/content/drive/MyDrive/Datasets/log_ejecucion.txt', 'a') as f:
    exc = format_exc()
    fecha = str(datetime.now())
    log =  fecha + ';' +'Error al almacenar el archivo registros_eps_agrupados' + ';' + exc
    f.write(log)
###***********************************************************
#Descargar información de casos positivos Covid a través de la API
try:
  #!pip install sodapy
  from sodapy import Socrata
  client = Socrata("www.datos.gov.co", None)
  #Autenticación y selección del archivo
  results = client.get("gt2j-8ykr",limit = 10000000)
  casos_covid = pd.DataFrame.from_records(results)
except:
  with open('/content/drive/MyDrive/Datasets/log_ejecucion.txt', 'a') as f:
    exc = format_exc()
    fecha = str(datetime.now())
    log =  fecha + ';' +'Error al cargar el archivod de casos positivos covid desde la API' + ';' + exc
    f.write(log)
#Se identifican registros nulos
casos_covid_nulos = casos_covid.isnull().sum()
#Estandarizar columna recuperado
casos_covid['recuperado'] = casos_covid['recuperado'].replace(['fallecido'],'Fallecido')
#Filtro por los fallecidos
casos_covid = casos_covid[casos_covid['recuperado'] == 'Fallecido']
#Crear una nueva columna con el gupo etario según la edad
try: 
  casos_covid['edad'] = casos_covid['edad'].astype(float)
  lista_condiciones = [
  (casos_covid['edad'] <= 5),
  (casos_covid['edad'] > 5) & (casos_covid['edad'] <=15),  
  (casos_covid['edad'] > 15) & (casos_covid['edad'] <=19),
  (casos_covid['edad'] > 19) & (casos_covid['edad'] <=55),
  (casos_covid['edad'] > 55) & (casos_covid['edad'] <=75),  
  (casos_covid['edad'] > 75)]
  lista_opcines = ['Primera Infancia','Infancia','Adolescencia','Adultez','Persona Mayor','Anciano']
  casos_covid['grupo_etario_cc'] = np.select(lista_condiciones, lista_opcines, default='Error')
except:
  with open('/content/drive/MyDrive/Datasets/log_ejecucion.txt', 'a') as f:
    exc = format_exc()
    fecha = str(datetime.now())
    log =  fecha + ';' +'Error al crear el grupo etario' + ';' + exc
    f.write(log)
casos_covid_agrupados = casos_covid.groupby(by = ["departamento","departamento_nom","grupo_etario_cc"])['edad'].count()

try:
  #Se almacena localmente el resultado
  fecha = datetime.now()
  nom_arc_casos_covid = "casos_covid_agrup_" + str(fecha.year) + str(fecha.month).rjust(2,'0') + str(fecha.day).rjust(2,'0') +".csv"
  casos_covid_agrupados.to_csv('/content/drive/MyDrive/Datasets/'+nom_arc_casos_covid,sep=';')
  #Enviar resultado de casos covid agrupados a la nube de google
  storage_client = storage.Client.from_service_account_json("/content/drive/MyDrive/Datasets/ingdatosnequi-8799c08e6f65.json")
  bucket_name = 'datasets_ing_datos'
  bucket = storage_client.get_bucket(bucket_name)
  filename = "output/" + nom_arc_casos_covid #nombre archivo bucket
  blop = bucket.blob(filename)
  blop.upload_from_filename('/content/drive/MyDrive/Datasets/'+nom_arc_casos_covid)
except:
  with open('/content/drive/MyDrive/Datasets/log_ejecucion.txt', 'a') as f:
    exc = format_exc()
    fecha = str(datetime.now())
    log =  fecha + ';' +'Error al almacenar el archivo casos_covid_agrupados' + ';' + exc
    f.write(log)
