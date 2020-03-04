import pandas as pd
import json
from tqdm import tqdm
import firebase_admin
from firebase_admin import credentials,firestore


csvFile = './data/matriculas.csv'
#agrupar = '{nestedJson}'


#Creamos un dataframe a partir del archivo csv
df = pd.read_csv(csvFile, delimiter=';',low_memory=False,encoding='utf8')
print('Archivo leido')
print(df.dtypes)

#progressBar
tqdm.pandas(desc='Creando bastidorSeries')

#Agrupamos el dataframe por nº Bastidor y lo exportamos a json
bastidorSeries = df.groupby(agrupar).progress_apply(lambda x : x.drop([agrupar],axis=1).to_json())


fullDict = {}
for keyBastidor, valueBastidor in tqdm(bastidorSeries.iteritems(),desc='Main Loop',total=len(bastidorSeries)):
    b = json.loads(valueBastidor)
    subDict = {}
    for keySub, valueSub in b.items():
        for i in valueSub:
            subDict[keySub] = valueSub[i]
    fullDict[keyBastidor] = subDict

'''print('\nComenzamos a volcar información en el json')
with open('2015_4m.json', 'w') as f:
    json.dump(fullDict, f)
print('Se ha volcado el json en el archivo')'''

#FIRESTORE
print('\nUploading task started...')
databaseURL = {'databaseURL' : "https://firesstore-test.firebaseio.com"}

cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)

count = 0
database = firestore.client()
for i,j in tqdm(fullDict.items(),desc='Uploading to FireStore'):
    collection_ref = database.collection('Matriculas').document(i).set(j)
    count += 1

print('Se han subido {} documentos'.format(count))



