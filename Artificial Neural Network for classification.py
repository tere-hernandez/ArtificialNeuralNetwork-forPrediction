# -*- coding: utf-8 -*-
"""
Created on Tue Dec  3 12:27:35 2019

Object: Obtener la información que se le pasará a la red neuronal para la predicción del número de goles

@author: carvi
"""



import requests
import json 
import pandas as pd
from pandas.io.json import json_normalize

import numpy as np
from datetime import date, datetime

import os


# API's data for get the information for analyze.
urlAPI = ''
headers = {
        'X-RapidAPI-Key' : '',
        'Accept' : 'application/json'
        }

flg_Load_LeaguesSeasons = False



def getRequestFromAPI(sectionName, firstNodeName, outputNormalize):
    
    urlGET = urlAPI + sectionName
    respRequest = requests.get(urlGET, '', headers = headers)
    dataRequest = json.loads(respRequest.text)
    
    if outputNormalize:
        dataRequest = json_normalize(dataRequest['api'][firstNodeName])
    else:
        dataRequest = dataRequest['api'][firstNodeName]
        
    return dataRequest


def getDataFromLocalFolder(workPath, fileName):
    
    print('Path specified: ' + workPath)
    dataTemp = pd.DataFrame()
    
    print('The following files will be read: ')
    # All files founded in the folder, will be upload
    for s in (s for s in os.listdir(workPath) if s.find(fileName) >= 0):
        print(s)
        dataTemp = pd.concat([dataTemp, 
                             pd.read_csv(s, low_memory = False).reset_index(drop=True)], axis = 0, sort = False)
            
    # Drop the first column because is the original index    
    dataTemp = dataTemp.iloc[:,1:]
    
    # Drop possible duplicated values
    dataTemp.drop_duplicates(keep = 'first', inplace = True)

    return dataTemp

    

print('\nOBTENIENDO CATÁLOGO DE LIGAS:')
cat_leagues = getRequestFromAPI('/leagues', 'leagues', True)
cat_leagues = cat_leagues[(cat_leagues['coverage.fixtures.players_statistics'] == True) & (cat_leagues['type'] == 'League')].sort_values(by=['league_id','country','name','season'])
cat_leagues.to_csv('cat_leagues.csv')

print('\nOBTENIENDO CATÁLOGO DE TEMPORADAS:')
cat_player_seasons = pd.DataFrame(data = getRequestFromAPI('/players/seasons', 'players', False), columns = ['season'])
cat_player_seasons.to_csv('cat_player_seasons.csv')





print ('\nOBTENIENDO INFORMACIÓN PREVIAMENTE CARGADA:\n')
# Load data from CSV files
cat_leagues = getDataFromLocalFolder(os.getcwd(), 'cat_leagues')
cat_player_seasons = getDataFromLocalFolder(os.getcwd(), 'cat_player_seasons')
data_players_statistics_fixtures = getDataFromLocalFolder(os.getcwd(), 'data_players_statistics_fixtures')
data_statistics_fixtures = getDataFromLocalFolder(os.getcwd(), 'data_statistics_fixtures')
data_lineups = getDataFromLocalFolder(os.getcwd(), 'data_lineups')
data_rounds_leagues = getDataFromLocalFolder(os.getcwd(), 'data_rounds_leagues')
data_fixtures = getDataFromLocalFolder(os.getcwd(), 'data_fixtures')


# Clean the data of lineups, because there are many errors in the information
dataTemp = data_lineups[['league_id','team_id','fixture_id','player_id','player']].groupby(by = ['league_id','team_id','fixture_id','player_id']).count()
dataTemp = dataTemp.reset_index()
dataTemp = dataTemp[dataTemp['player'] > 1]

data_lineups = pd.merge(
        data_lineups,
        dataTemp[['league_id','team_id','fixture_id','player_id']],
        how = 'left',
        on = ['league_id','team_id','fixture_id','player_id'],
        indicator = 'join_result')

data_lineups = data_lineups.drop(index = data_lineups[(data_lineups['join_result'] == 'both')].index)
data_lineups = data_lineups.drop(columns = 'join_result')



listAPIRequest = []

def getRoundsFromAPIByLeague(league_id, sectionName, firstNodeName):
    print('      Rounds:')
    flag_LeagueId = True
    
    try:
        # The dataframe doesn't exists or doesn't have the column
        if (len(data_rounds_leagues[data_rounds_leagues['league_id'] == league_id]) != 0):
            
            flag_LeagueId = False
            print('        Los datos de la liga solicitada ya habían sido cargados.')
            
    except NameError:
        pass        


    if (flag_LeagueId):
        
        dataRequest = pd.DataFrame(data = getRequestFromAPI(sectionName, firstNodeName, False), columns = {'round'})
        dataRequest = dataRequest.applymap(lambda x: x.replace('_-_', ' - ')).applymap(lambda x: x.replace('_', ' '))
                
        dataRequest_temp =  dataRequest['round'].str.split(' - ', n = 1, expand = True)
        dataRequest['league_id'] = league_id
        dataRequest['round_type'] = dataRequest_temp[0]
        dataRequest['round_number'] = dataRequest_temp[1]
        dataRequest['consecutive'] = dataRequest.groupby('round_type', as_index = False).cumcount() + 1
        
        dataRequest = pd.merge(dataRequest, 
                               dataRequest[['round_type','consecutive']].groupby('round_type', as_index = False).max().rename(columns = {'consecutive':'num_rounds'}),
                               how = 'inner',
                               on = ['round_type'])
        
        dataRequest['season_completion'] = (dataRequest['consecutive'] * 100) / dataRequest['num_rounds']
        
        data_rounds_leagues = pd.concat([data_rounds_leagues, dataRequest], axis = 0, sort = False)            
            
          
        
        
# -----------
# A continuación obtengo la información de la API con base en los ID de liga obtenidos en pasos  anteriores
# -----------
print ('\nOBTENIENDO INFORMACIÓN DE LA API:\n')


dataTemp = None
flag_LeagueId = False
flag_OutputCSVFiles = True
count_apirequests = 0
dateLimitAnalasysMatches = datetime.now()

print()

for idx, e in cat_leagues.iterrows():
    
    print('   Liga ID: ' + str(e.league_id) 
            + ' ' + str(e['name']) 
            + ' from ' + str(e.country) 
            + ' (' + str(e.season) + ')')

    # ------
    # ROUNDS
    # ------
    data_rounds_leagues = getRoundsFromAPIByLeague(e.league_id, '/fixtures/rounds/', 'fixtures')
    
    
    print('      Rounds:')
    
    # Valido si ya hay rounds cargados en la tabla
    flag_LeagueId = False
    
    if (('league_id' not in data_rounds_leagues.columns)): # El DataFrame no tiene información cargada
        flag_LeagueId = True
    else:
        if (len(data_rounds_leagues[data_rounds_leagues['league_id'] == e.league_id]) == 0):
            flag_LeagueId = True
        else:
            flag_LeagueId = False
            print('        Los datos de esta liga ya habían sido cargados.')
        

    if (flag_LeagueId):
        count_apirequests += 1
        urlGET = urlAPI + '/fixtures/rounds/' + str(e.league_id)
        respRequest = requests.get(urlGET,"",headers = headers)
        dataRequest = json.loads(respRequest.text)
        
        if dataRequest['api']['results'] != 0:
            dataRequest = pd.DataFrame(data = dataRequest['api']['fixtures'], columns = {'round'})
            dataRequest = dataRequest.applymap(lambda x: x.replace('_-_', ' - '))
            dataRequest = dataRequest.applymap(lambda x: x.replace('_', ' '))
    
            
            dataRequest_temp =  dataRequest['round'].str.split(' - ', n = 1, expand = True)
            dataRequest['league_id'] = e.league_id
            dataRequest['round_type'] = dataRequest_temp[0]
            dataRequest['round_number'] = dataRequest_temp[1]
            dataRequest['consecutive'] = dataRequest.groupby('round_type', as_index = False).cumcount() + 1
            
            dataRequest = pd.merge(dataRequest, 
                                   dataRequest[['round_type','consecutive']].groupby('round_type', as_index = False).max().rename(columns = {'consecutive':'num_rounds'}),
                                   how = 'inner',
                                   on = ['round_type'])
            
            dataRequest['season_completion'] = (dataRequest['consecutive'] * 100) / dataRequest['num_rounds']
            
            data_rounds_leagues = pd.concat([data_rounds_leagues, dataRequest], axis = 0, sort = False)            
                
            print ('         OK')
                

    # ------
    # FIXTURES
    # ------
    print('      Partidos (fixtures):')

    if (('league_id' not in data_fixtures.columns)):
            flag_LeagueId = True
    else:
        if (len(data_fixtures[data_fixtures['league_id'] == e.league_id]) == 0):
            flag_LeagueId = True
        else:
            flag_LeagueId = False
            print('        Los datos de esta liga ya habían sido cargados.')
            
    
    if (flag_LeagueId):
        count_apirequests += 1
        urlGET = urlAPI + '/fixtures/league/' + str(e.league_id)
        respRequest = requests.get(urlGET,"",headers = headers)
        dataRequest = json.loads(respRequest.text)
        if (dataRequest["api"]["results"] != 0):
            dataRequest = json_normalize(dataRequest["api"]["fixtures"])
            dataRequest['season'] = e.season
            
            
            # Validación de posibles datos duplicados
            dataTemp = dataRequest[['league_id','fixture_id','league.name']].groupby(by=['league_id','fixture_id']).count()
            dataTemp = dataTemp[dataTemp['league.name'] > 1].reset_index()
            
            dataRequest = pd.merge(
                    dataRequest,
                    dataTemp[['league_id','fixture_id']],
                    how = 'left',
                    on = ['league_id','fixture_id'],
                    indicator = 'join_result'
                    )
            
            # Elimino las filas que están duplicadas y que tienen valores nulos
            dataRequest = dataRequest.drop(index = dataRequest[
                    (dataRequest[['goalsHomeTeam']].isnull().any(axis = 1))
                    & (dataRequest['join_result'] == 'both')].index)
        
            dataRequest = dataRequest.drop(columns = ['join_result'])
            
            data_fixtures = pd.concat([data_fixtures, dataRequest], axis = 0, sort = False)
        else:
            dataRequest = None

    

    print ('         OK')
    
    dataFixturesTemp = data_fixtures[(data_fixtures['league_id'] == e.league_id) & (pd.to_datetime(data_fixtures['event_date']) <= str(dateLimitAnalasysMatches))]
    numFixturesExpected = len(dataFixturesTemp)
    


    
    # ------
    # PLAYERS STATISTICS FIXTURES
    # ------  


    dataStatisticsFixture = data_players_statistics_fixtures[data_players_statistics_fixtures['league_id'] == e.league_id].fixture_id.unique()
    numFixturesGetted = len(dataStatisticsFixture)
    
    print('      Estadísticas de jugadores en partidos (players statistics fixtures):')
    
    # Verifico si hay ligas con partidos incompletos
    if (('league_id' not in data_players_statistics_fixtures.columns)):
            flag_LeagueId = True
    else:        
        if numFixturesExpected > numFixturesGetted:

            dataTemp = dataFixturesTemp[~dataFixturesTemp['fixture_id'].isin(dataStatisticsFixture)]
            print('         Num. partidos previamente almacenados: ' + str(numFixturesGetted)
                  + ' de ' + str(numFixturesExpected) + '. Se obtendrán: ' + str(len(dataTemp)) )
            
            flag_LeagueId = True
            
        else:
            
            dataTemp = None
            flag_LeagueId = False
            print('         Los datos disponibles de esta liga ya habían sido cargados (total esperado: ' + str(numFixturesExpected) + ', total disponible: ' + str(numFixturesGetted) + ')')
            

    if (flag_LeagueId):
        # Busco la información de los partidos que no he leido
        for idxf, f in dataTemp.iterrows():            

            count_apirequests += 1
            print('         API request realizadas por ciclo: num. ' + str(count_apirequests) + ' league_id:' + str(f.league_id) + ' | ' + 'fixture_id: ' + str(f.fixture_id) + ' | ' + 'event_date: ' + str(f.event_date))                    
            urlGET = urlAPI + '/players/fixture/' + str(f.fixture_id)
            respRequest = requests.get(urlGET,"",headers = headers)
            dataRequest = json.loads(respRequest.text)

            if (dataRequest["api"]["results"] != 0):
                dataRequest = json_normalize(dataRequest["api"]["players"])
                dataRequest['league_id'] = f.league_id
                dataRequest['fixture_id'] = f.fixture_id                

                data_players_statistics_fixtures = pd.concat([data_players_statistics_fixtures, dataRequest], axis = 0, sort = False)
            else:
                print('            * Sin información.')
                dataRequest = None
        
    print ('         OK')
    
    

    # ------
    # STATISTICS FIXTURES
    # ------
    print('      Estadísticas de partido (statistics fixtures):')
    
    dataStatisticsFixture = data_statistics_fixtures[data_statistics_fixtures['league_id'] == e.league_id].fixture_id.unique()
    numFixturesGetted = len(dataStatisticsFixture)
    
    
    # Verifico si hay ligas con partidos incompletos
    if (('league_id' not in data_statistics_fixtures.columns)):
            flag_LeagueId = True
            
    else:

        if numFixturesExpected > numFixturesGetted:
            
            dataTemp = dataFixturesTemp[~dataFixturesTemp['fixture_id'].isin(dataStatisticsFixture)]
            print('         Num. partidos previamente almacenados: ' + str(numFixturesGetted)
                              + ' de ' + str(numFixturesExpected) + '. Se obtendrán: ' + str(len(dataTemp)) )
            
            flag_LeagueId = True            
            
        else:
            
            dataTemp = None
            flag_LeagueId = False
            print('         Los datos disponibles de esta liga ya habían sido cargados (total esperado: ' + str(numFixturesExpected) + ', total disponible: ' + str(numFixturesGetted) + ')')

                
    if (flag_LeagueId):
        # Busco la información de los partidos que no he leido
        for idxf, f in dataTemp.iterrows():            
            
            count_apirequests += 1
            print('         API request realizadas por ciclo: num. ' + str(count_apirequests) + ' league_id:' + str(f.league_id) + ' | ' + 'fixture_id: ' + str(f.fixture_id) + ' | ' + 'event_date: ' + str(f.event_date))                    
            urlGET = urlAPI + '/statistics/fixture/' + str(f.fixture_id)
            respRequest = requests.get(urlGET,"",headers = headers)
            dataRequest = json.loads(respRequest.text)

            if (dataRequest["api"]["results"] != 0):
                dataRequest = json_normalize(dataRequest["api"]["statistics"])
                dataRequest['league_id'] = f.league_id
                dataRequest['fixture_id'] = f.fixture_id
                data_statistics_fixtures = pd.concat([data_statistics_fixtures, dataRequest], axis = 0, sort = False)  
                
            else:
                print('            * Sin información.')
                dataRequest = None
                
    print ('         OK')


    
    # dataFixturesTemp = data_fixtures[(data_fixtures['league_id'] == e.league_id) & (pd.to_datetime(data_fixtures['event_date']) <= str(date.today()))]
    dataFixturesTemp = data_fixtures[(data_fixtures['league_id'] == e.league_id) & (pd.to_datetime(data_fixtures['event_date']) <= str(dateLimitAnalasysMatches))]
    numFixturesExpected = len(dataFixturesTemp)

    # ------
    # LINEUPS
    # ------
    print('      Alineaciones (lineups):')
    
    # Verifico si hay ligas con partidos incompletos
    if (('league_id' not in data_lineups.columns)):
        flag_LeagueId = True
        dataLineUps = None
        dataTemp = dataFixturesTemp
        numLineUpsGetted = 0
            
    else:
        dataLineUps = data_lineups[(data_lineups['league_id'] == e.league_id)]
        dataLineUps = dataLineUps[['league_id','fixture_id','player_id']].groupby(by = ['league_id','fixture_id']).count().reset_index()
        dataLineUps = dataLineUps[dataLineUps['player_id'] == 22].fixture_id.unique()
        
        numLineUpsGetted = len(dataLineUps)

        if numFixturesExpected > numLineUpsGetted:
            
            dataTemp = dataFixturesTemp[~dataFixturesTemp['fixture_id'].isin(dataLineUps)]
            print('         Num. partidos previamente almacenados: ' + str(numLineUpsGetted)
                              + ' de ' + str(numFixturesExpected) + '. Se obtendrán: ' + str(len(dataTemp)) )
            
            flag_LeagueId = True            
            
        else:
            
            dataTemp = None
            flag_LeagueId = False
            print('         Los datos disponibles de esta liga ya habían sido cargados (total esperado: ' + str(numFixturesExpected) + ', total disponible: ' + str(numLineUpsGetted) + ')')

                
    if (flag_LeagueId):
        # Busco la información de los partidos que no he leido
        for idxf, f in dataTemp.iterrows():            
            
            count_apirequests += 1
            print('         API request realizadas por ciclo: num. ' + str(count_apirequests) + ' league_id:' + str(f.league_id) + ' | ' + 'fixture_id: ' + str(f.fixture_id) + ' | ' + 'event_date: ' + str(f.event_date))
            urlGET = urlAPI + '/lineups/' + str(f.fixture_id)
            respRequest = requests.get(urlGET,"",headers = headers)
            dataRequest = json.loads(respRequest.text)
                        
            if (dataRequest["api"]["results"] != 0):
                
                for s in dataRequest["api"]["lineUps"]:
                    
                     if ('startXI' in dataRequest["api"]["lineUps"][s].keys()):
                    
                        dataTemp2 = json_normalize(dataRequest["api"]["lineUps"][s]['startXI'])
                        dataTemp2.insert(0, 'league_id', f.league_id)
                        dataTemp2.insert(1, 'fixture_id', f.fixture_id)
                        
                        if ('formation' in dataRequest["api"]["lineUps"][s].keys()):
                            dataTemp2['formation'] = dataRequest["api"]["lineUps"][s]['formation']
                        else:
                            dataTemp2['formation'] = ''
                        
                        data_lineups = pd.concat([data_lineups, dataTemp2], axis = 0, sort = False)
                        
                     else:
                        print('               * Sin información.')
                    
            else:
                print('            * Sin información.')
                dataRequest = None
                

    print ('         OK')


# Reviso consistencia de la información

# FIXTURES
dataTemp = (data_fixtures[['fixture_id','league_id']].groupby(by=['fixture_id']).count()).reset_index()
print ('Partidos repetidos: ' + str(len(dataTemp[dataTemp['league_id'] > 1])))

# ROUNDS
dataTemp = (data_rounds_leagues[['league_id','round','round_type']].groupby(by=['league_id','round']).count()).reset_index()
print ('Rounds repetidos: ' + str(len(dataTemp[dataTemp['round_type'] > 1])))

# ESTADÍSTICAS DE PARTIDOS
dataTemp = (data_statistics_fixtures[['league_id','fixture_id','Shots on Goal.home']].groupby(by=['league_id','fixture_id']).count()).reset_index()
print ('Estadísticas de partido repetidas: ' + str(len(dataTemp[dataTemp['Shots on Goal.home'] > 1])))

# ESTADÍSTICAS DE JUGADORES POR PARTIDO
dataTemp = (data_players_statistics_fixtures[['league_id','fixture_id','team_id', 'player_id','position','player_name']].groupby(by=['league_id','fixture_id','team_id', 'player_id','position']).count()).reset_index()
print ('Estadísticas de jugadores repetidas: ' + str(len(dataTemp[dataTemp['player_name'] > 1])))

# ALINEACIONES REPETIDAS
dataTemp = (data_lineups[['league_id','fixture_id','team_id', 'player_id','player']].groupby(by=['league_id','fixture_id','team_id', 'player_id']).count()).reset_index().rename(columns = {'player':'val_player'})
dataTemp = pd.merge(
        data_lineups,
        dataTemp[dataTemp['val_player'] > 1][['league_id','fixture_id','team_id','player_id']],
        how = 'inner',
        on = ['league_id','fixture_id','team_id','player_id'])
print ('Alineaciones de partido repetidas: ' + str(len(dataTemp)))




# Almacenamiento de información en archivos csv
if flag_OutputCSVFiles:
    
    data_rounds_leagues.to_csv('data_rounds_leagues_leagueid' + str(e.league_id) + '.csv')
    print('         data_rounds_leagues en archivo .csv OK')

    data_fixtures.to_csv('data_fixtures_leagueid' + str(e.league_id) + '.csv')
    print('         data_fixtures en archivo .csv OK')

    data_players_statistics_fixtures.to_csv('data_players_statistics_fixtures_leagueid' + str(e.league_id) + '.csv')
    print('         data_players_statistics_fixtures en archivo .csv OK')

    data_statistics_fixtures.to_csv('data_statistics_fixtures_leagueid' + str(e.league_id) + '.csv')
    print('         data_statistics_fixtures en archivo .csv OK')
    
    data_lineups.to_csv('data_lineups_leagueid' + str(e.league_id) + '.csv')
    print('         data_lineups en archivo .csv OK')




# --------------------
# Uno todas las piezas del rompecabezas
# --------------------
# Sólo puedo trabajar con los partidos que tienen tanto estadísitcas de partido como estadísticas de jugadores
    

data_ANN_fixturesId = pd.merge(
        data_fixtures['fixture_id'],
        data_statistics_fixtures['fixture_id'],
        how = 'left',
        on = 'fixture_id'
        #,indicator = 'aa'
        )


data_ANN_fixturesId = pd.merge(
        data_ANN_fixturesId,
        pd.DataFrame(data_players_statistics_fixtures['fixture_id'].unique(), columns = ['fixture_id']),
        how = 'left',
        on = 'fixture_id'
        #,indicator = 'aa'
        )


# Filtro los dataframes con los fiture_id encontrados
data_ann_fixtures = data_fixtures[data_fixtures['fixture_id'].isin(data_ANN_fixturesId['fixture_id'])]
data_ann_statistics_fixtures = data_statistics_fixtures[data_statistics_fixtures['fixture_id'].isin(data_ANN_fixturesId['fixture_id'])]
data_ann_players_statistics_fixtures = data_players_statistics_fixtures[data_players_statistics_fixtures['fixture_id'].isin(data_ANN_fixturesId['fixture_id'])]




# Agrego la información de estadisticas por jugador
# Ignrados: goals.total, goals.conceded, goals.assists, penalty.won, penalty.commited, penalty.success, penalty.missed, penalty.saved
# data_ann_players_statistics_fixtures.info()
data_ann_players_statistics_fixtures = data_ann_players_statistics_fixtures[['league_id','fixture_id','team_id',
                                  'position','player_id','captain','substitute','rating','minutes_played',
                                  'shots.total','shots.on','passes.total','passes.key','passes.accuracy',
                                  'tackles.total','tackles.blocks','tackles.interceptions',
                                  'duels.total','duels.won','dribbles.attempts','dribbles.success','dribbles.past',
                                  'fouls.drawn','fouls.committed','cards.yellow','cards.red']]




# En caso de que la API no haya marcado correctamente a los primeros 11 jugadores, yo los marcaré
# Limpio la información de la tabla de alineaciones
data_lineups.drop_duplicates(keep = 'first', inplace = True)

# En RATING, los valores nulos y - los convierto en la media del equipo
#data_ann_players_statistics_fixtures = data_ann_players_statistics_fixtures[data_ann_players_statistics_fixtures['rating'].notnull()]
data_ann_players_statistics_fixtures['rating'] = data_ann_players_statistics_fixtures['rating'].apply(lambda x: np.nan if x == '–' else x)

# Convierto la columna a FLOAT
data_ann_players_statistics_fixtures['rating'] = data_ann_players_statistics_fixtures['rating'].astype(float)

data_ann_players_statistics_fixtures['rating'] = data_ann_players_statistics_fixtures['rating'].apply(lambda x: np.nan if x == 0 else x)

# Cambio nulos por la media de los jugadores por equipo
data_ann_players_statistics_fixtures['rating'] = data_ann_players_statistics_fixtures[['league_id','team_id','rating']].groupby(by = ['league_id','team_id']).transform(lambda x: x.fillna(x.mean()))


data_ann_players_statistics_fixtures = pd.merge(
        data_ann_players_statistics_fixtures,
        data_lineups[['league_id','fixture_id','team_id','player_id','formation']],
        how = 'left',
        on = ['league_id','fixture_id','team_id','player_id']
        ,indicator = 'd'
        )


# Obtengo la marca correcta de sustitutos con base a la tabla de alineaciones
data_ann_players_statistics_fixtures.loc[:, 'substitute'] = data_ann_players_statistics_fixtures.apply(lambda x: False if x['d'] == 'both' else True, axis = 1)

# Agrego numeración a los jugadores para identificar a los primeros 11
data_ann_players_statistics_fixtures.insert(3, 'num_player', data_ann_players_statistics_fixtures.groupby(['league_id','fixture_id','team_id']).cumcount().add(1))

# Corrijo inconsistencias de alineación. Ya que hay jugadores que están dentro del grupo de los primeros 11 que están marcados como sustitutos
data_ann_players_statistics_fixtures.loc[:, 'substitute'] = data_ann_players_statistics_fixtures[['substitute','num_player']].apply(lambda x: False if (x['substitute'] == True) & (x['num_player'] <= 11) else x['substitute'], axis = 1)


data_ann_players_statistics_fixtures.insert(len(data_ann_players_statistics_fixtures.columns),
                                            'formation2',
                                            data_ann_players_statistics_fixtures['formation']
                                            )

#data_ann_players_statistics_fixtures.info()

# Relleno el dato de formación para aquellos jugadores que tuvieron problemas en la selcción de 
data_ann_players_statistics_fixtures['formation'] = data_ann_players_statistics_fixtures[['league_id','fixture_id','team_id','formation']].groupby(by = ['league_id','fixture_id','team_id']).transform(lambda x: x.fillna(method = 'ffill'))
data_ann_players_statistics_fixtures['formation'] = data_ann_players_statistics_fixtures[['league_id','fixture_id','team_id','formation']].groupby(by = ['league_id','fixture_id','team_id']).transform(lambda x: x.fillna(method = 'bfill'))
# Los partidos que quedan son los que definitivamente no hayinformación de la formación
data_ann_players_statistics_fixtures['formation'] = data_ann_players_statistics_fixtures[['league_id','fixture_id','team_id','formation']].groupby(by = ['league_id','fixture_id','team_id']).transform(lambda x: x.fillna(''))



# Debido a que el resto del análisis sólo se basará en los primeros 11, elimino a los jugadores substitutos
data_ann_players_statistics_fixtures = data_ann_players_statistics_fixtures[data_ann_players_statistics_fixtures['num_player'] <= 11]

# Agrego la columna Tipo de jugador: 1) Capitán, 2) Primeros 11, 3) Sustituto
data_ann_players_statistics_fixtures.insert(5, 'type_player', 0)
data_ann_players_statistics_fixtures.loc[:, 'type_player'] = data_ann_players_statistics_fixtures.apply(lambda x: 1 if x['captain'] == True else (3 if x['substitute'] == True else 2), axis=1)

# Homologo valores de posiciones
data_ann_players_statistics_fixtures.loc[~data_ann_players_statistics_fixtures['position'].isin(['G','D','M','F']),'position'] = 'O'

# Homologo las posiciones: 1 = G, 2 = Defensa, 3 = Medio, 4 = Delantero
data_ann_players_statistics_fixtures.insert(
        len(data_ann_players_statistics_fixtures.columns), 
        'id_position', 
        data_ann_players_statistics_fixtures.apply(lambda x: 1 if x['position'] == 'G' else
            (2 if x['position'] == 'D' else
                (3 if x['position'] == 'M' else
                    (4 if x['position'] == 'F' else 0))), axis = 1)
        )
        
data_ann_players_statistics_fixtures.insert(
        len(data_ann_players_statistics_fixtures.columns), 
        'position_order', 
        data_ann_players_statistics_fixtures.apply(lambda x: 1 if x['position'] == 'G' else 2, axis = 1))
        


# Ordeno la información de la siguiente manera de los primeros 11: 1) Portero 2) Resto orderados por rating 
data_ann_players_statistics_fixtures.sort_values(by = [
        'league_id',
        'fixture_id',
        'team_id',
        'substitute',
        #data_ann_players_statistics_fixtures.apply(lambda x: 1 if x['position'] == 'G' else 2, axis = 1),
        'position_order',
        'rating'],
    ascending = [True, True, True, True, True, False],
    inplace = True
    )



# Modifico columna índice por equipo:
data_ann_players_statistics_fixtures['num_player'] = data_ann_players_statistics_fixtures.groupby(['league_id','fixture_id','team_id']).cumcount().add(1)


# Convierto a clave la información de la formación
cat_formations = pd.DataFrame(data_ann_players_statistics_fixtures['formation'].unique(), columns = ['formation']).sort_values('formation').reset_index(drop = True)
cat_formations.insert(0, 'formation_id', cat_formations.index)

# Agrego formación del partido
data_ann_players_statistics_fixtures = pd.merge(
        data_ann_players_statistics_fixtures,
        cat_formations,
        how = 'inner',
        on = 'formation')



# Me quedo con los datos que necesito para la ANN
data_ann_players_statistics_fixtures_t = pd.DataFrame()
data_ann_players_statistics_fixtures_t = data_ann_players_statistics_fixtures[['league_id','fixture_id','team_id','formation_id']].drop_duplicates(keep = 'first').sort_values(by = ['league_id','fixture_id','team_id'])


#data_ann_players_statistics_fixtures.info()
data_ann_players_statistics_fixtures.drop(columns = ['position','player_id','captain','substitute','minutes_played',
                                                     'formation','d','formation2','position_order','formation_id'], inplace = True)

# Agrego la información de los jugadores
i = 1
while i <= 11: # Por el número de jugadores que debe ingresar a la cancha en el inicio del parido
    
    data_ann_players_statistics_fixtures_t = pd.merge(
            data_ann_players_statistics_fixtures_t,
            data_ann_players_statistics_fixtures[data_ann_players_statistics_fixtures['num_player'] == i].add_prefix(('p' + str(i) + '.')),
            how = 'inner',
            left_on = ['league_id','fixture_id','team_id'],
            right_on = ['p' + str(i) + '.league_id'
                        ,'p' + str(i) + '.fixture_id'
                        ,'p' + str(i) + '.team_id']
            ).drop(columns = ['p' + str(i) + '.league_id','p' + str(i) + '.fixture_id','p' + str(i) + '.team_id',
                  'p' + str(i) + '.num_player'])
    
    i += 1






# Datos generales del partido
# data_ann_fixtures[['league_id','event_date']].groupby('league_id').max()
data_ann_input = data_ann_fixtures[['league.name','league.country','season','league_id','fixture_id','round','event_date',
                                    'homeTeam.team_id','homeTeam.team_name','awayTeam.team_id','awayTeam.team_name',
                                    'goalsHomeTeam','goalsAwayTeam']]


data_ann_input = pd.merge(
        data_ann_input,
        data_rounds_leagues[['league_id','round','season_completion']],
        how = 'left',
        on = ['league_id','round']
        #, indicator = 'bb'
        ).drop_duplicates(keep = 'first')



# Estadísticas del partido
# data_statistics_fixtures.info()

# Por jugador:  'Shots on Goal.home','Shots off Goal.home', Fouls.home, Yellow Cards.home, Red Cards.home, Goalkeeper Saves.home
#               'Shots on Goal.away','Shots off Goal.away','Total passes.home', 'Passes accurate.home',
#               'Total passes.away','Passes accurate.away'
# Ignoradas: Assists.home, Goals.home, Goal Attempts.home, 'Total Shots.home', 'Passes %.home',
# Sin información: 'Counter Attacks.home','Cross Attacks.home','Free Kicks.home','Substitutions.home','Throwins.home','Medical Treatment.home'
data_ann_input = pd.merge(
        data_ann_input,
        data_statistics_fixtures[['league_id','fixture_id',
                                  'Shots insidebox.home','Shots outsidebox.home',
                                  'Blocked Shots.home',
                                  'Corner Kicks.home','Offsides.home',
                                  'Ball Possession.home',
                                  'Shots insidebox.away','Shots outsidebox.away',
                                  'Blocked Shots.away',
                                  'Corner Kicks.away','Offsides.away',
                                  'Ball Possession.away',
                                  'Yellow Cards.home','Yellow Cards.away',
                                  'Red Cards.home','Red Cards.away'
                                  ]].fillna(0),
        how = 'left',
        on = ['league_id','fixture_id']
        # , indicator = 'c'
        )


data_ann_input.fillna(value = -1, axis = 1, inplace = True)


data_ann_input['Ball Possession.home'] = np.where(
        data_ann_input['Ball Possession.home'].str.contains('%', na = False), 
        data_ann_input['Ball Possession.home'].str.replace('%',''),
        data_ann_input['Ball Possession.home']).astype(np.int32)

data_ann_input['Ball Possession.away'] = np.where(
        data_ann_input['Ball Possession.away'].str.contains('%', na = False), 
        data_ann_input['Ball Possession.away'].str.replace('%',''),
        data_ann_input['Ball Possession.away']).astype(np.int32)



# Estadísticas de jugadores equipos locales
data_ann_input = pd.merge(
        data_ann_input,
        data_ann_players_statistics_fixtures_t.add_suffix('.home'),
        how = 'left',
        left_on = ['league_id','fixture_id','homeTeam.team_id'],
        right_on = ['league_id.home','fixture_id.home','team_id.home'],
        indicator = 'homeInfo'
        ).drop(columns = ['league_id.home','fixture_id.home','team_id.home'])

# Estadísticas de jugadores equipos visitantes
data_ann_input = pd.merge(
        data_ann_input,
        data_ann_players_statistics_fixtures_t.add_suffix('.away'),
        how = 'left',
        left_on = ['league_id','fixture_id','awayTeam.team_id'],
        right_on = ['league_id.away','fixture_id.away','team_id.away'],
        indicator = 'awayInfo'
        ).drop(columns = ['league_id.away','fixture_id.away','team_id.away'])

# Reemplazo por -1 las celdas que no tienen valor por ser partidos que aún no tienen estadísticas
data_ann_input.replace(np.nan, -1, inplace = True)

# Me quedo con los partidos que tienen estadísticas de jugadores
data_ann_input.drop(columns = ['homeInfo','awayInfo'], inplace = True)

data_ann_input.sort_values(by = ['league.name','event_date','fixture_id'], ascending = [True, False, False], inplace= True)
# data_ann_input[['league_id','event_date']].groupby('league_id').max()



# ---------------------
# Calculo los nuevos campos de salida
# ---------------------
# Preparo información adicional que puede ser requerida por la RNN
data_ann_input.insert(13, 'realResult', data_ann_input.apply(lambda x: '' if x['goalsHomeTeam'] == -1 else ('E' if x['goalsHomeTeam'] == x['goalsAwayTeam'] else ('L' if x['goalsHomeTeam'] > x['goalsAwayTeam'] else 'V')), axis = 1))
# Los resultados que no pude calcular los pongo como empate para que mas adelante pueda obtener los grupos de convergencia de la red
data_ann_input['realResult'] = data_ann_input['realResult'].fillna('E')


## OPCIÓN 1: SALIDA PARA CADA COMBINACIÓN DE MARCADOR
# Máximo 6 goles por equipo
data_ann_input.insert(13, 'goalsTeams6', pd.to_numeric(data_ann_input[['goalsHomeTeam','goalsAwayTeam']].apply(
        lambda row: ('6' if row['goalsHomeTeam'] >= 6 else '0' if row['goalsHomeTeam'] == -1 else str(row['goalsHomeTeam'].astype('int32'))) 
        + ('6' if row['goalsAwayTeam'] >= 6 else '0' if row['goalsAwayTeam'] == -1 else str(row['goalsAwayTeam'].astype('int32'))), axis = 1)))

# Máximo 4 goles por equipo    
data_ann_input.insert(14, 'goalsTeams4', pd.to_numeric(data_ann_input[['goalsHomeTeam','goalsAwayTeam']].apply(
        lambda row: ('4' if row['goalsHomeTeam'] >= 4 else '0' if row['goalsHomeTeam'] == -1 else str(row['goalsHomeTeam'].astype('int32'))) 
        + ('4' if row['goalsAwayTeam'] >= 4 else '0' if row['goalsAwayTeam'] == -1 else str(row['goalsAwayTeam'].astype('int32'))), axis = 1)))


print(len(data_ann_input[data_ann_input['fixture_id'] == 293418]))
borrame = data_ann_input[data_ann_input['fixture_id'] == 293418]
borrame = borrame.drop_duplicates(keep = 'first')
borrame.diff(periods = 1, axis = 0)



# *******************
# ************ A PARTIR DE AQUI SE CARGA LA INFORMACIÓN 
# *******************    
data_ann_input.to_csv('data_ann_input.csv')

# Hago copias de la información para obtener el promedio de los partidos
data_ann_input_copy = data_ann_input.copy(deep = True)

# Quito filas con valores nulos y -1
len(data_ann_input[data_ann_input.isnull().any(axis = 1)])
len(data_ann_input[data_ann_input.isin([-1]).any(axis = 1)])

dataTemp = data_ann_input.drop(columns = ['goalsHomeTeam','goalsAwayTeam'])
dataTemp = dataTemp[dataTemp.isin([-1]).any(axis = 1)]

data_team_statisticts = data_ann_input.copy(deep = True)



# *******************
# Calculo un segundo data con la relación de estadísticas delpartido 1 con el 2, del 2 con el 3 y así
# *******************

# PREPARO LA INFORMACIÓN PARA TENER DESGLOSADO LOCAL Y VISITA 

# Datos Local
list_columns_avg = ['league.country','league_id','league.name','fixture_id','season','round','event_date','homeTeam.team_id','homeTeam.team_name','goalsHomeTeam','goalsAwayTeam','season_completion']
list_columns_avg.extend([x for x in list(data_team_statisticts.columns.values) if '.home' in x])

dataTemp = data_team_statisticts[list_columns_avg].rename(columns = {'homeTeam.team_id':'team_id','homeTeam.team_name':'team_name', 'goalsHomeTeam':'goalsTeam', 'goalsAwayTeam':'goalsReceived'})
dataTemp.columns = dataTemp.columns.str.replace('.home','') 
dataTemp.insert(11, 'home_away', 1)


# Datos visitante
list_columns_avg = ['league.country','league_id','league.name','fixture_id','season','round','event_date','awayTeam.team_id','awayTeam.team_name','goalsAwayTeam','goalsHomeTeam','season_completion']
list_columns_avg.extend([x for x in list(data_team_statisticts.columns.values) if '.away' in x])

dataTemp2 = data_team_statisticts[list_columns_avg].rename(columns = {'awayTeam.team_id':'team_id','awayTeam.team_name':'team_name', 'goalsAwayTeam':'goalsTeam', 'goalsHomeTeam':'goalsReceived'})
dataTemp2.columns = dataTemp2.columns.str.replace('.away','')
dataTemp2.insert(11, 'home_away', 2)

# Uno toda la información en dataTemp
dataTemp = pd.concat([dataTemp, dataTemp2], sort = False)


# Ordeno valores para calular las columnas pivote para obtener los registros necesarios para el promedio.
dataTemp.sort_values(by = ['team_id','event_date','season','fixture_id'],
                     ascending = [True, False, False, False],
                     inplace = True)

dataTemp.reset_index(inplace = True, drop = True)


# Agrego los siguientes datos acumulados por liga y temporada
# Num partidos ganados, num partidos empatados, num partidos perdidos, 
# num goles recibidos, num goles anotados, 
# num tarjetas amarillas, num tarjetas rojas
dataTemp.insert(11, 'is_fixtureWon', dataTemp.apply(lambda x: x['goalsTeam'] if x['goalsTeam'] == -1 else (1 if x['goalsTeam'] > x['goalsReceived'] else 0), axis = 1))
dataTemp.insert(12, 'is_fixtureTied', dataTemp.apply(lambda x: x['goalsTeam'] if x['goalsTeam'] == -1 else (1 if x['goalsTeam'] == x['goalsReceived'] else 0), axis = 1))
dataTemp.insert(13, 'is_fixtureLost', dataTemp.apply(lambda x: x['goalsTeam'] if x['goalsTeam'] == -1 else (1 if x['goalsTeam'] < x['goalsReceived'] else 0), axis = 1))

dataTemp.insert(15, 'num_fixturesWon', 0)
dataTemp.insert(16, 'num_fixturesTied', 0)
dataTemp.insert(17, 'num_fixturesLost', 0)
dataTemp.insert(18, 'num_YellowCards', 0)
dataTemp.insert(19, 'num_RedCards', 0)
dataTemp.insert(20, 'num_GoalsFor', 0)
dataTemp.insert(21, 'num_GoalsAgainst', 0)

# Agrego columna con el número de partido de forma descendente, el más reciente es el#
dataTemp.insert(2, 'num_fixture', dataTemp.groupby(['team_id','league.name']).cumcount().add(1))
dataTemp.insert(3, 'num_fixture2', dataTemp.apply(lambda x: x['num_fixture'] - 1, axis = 1))
dataTemp.insert(4, 'num_fixture_perLeagueTeam', dataTemp.groupby(['league_id','team_id']).cumcount().add(1))

dataTemp.insert(0, 'index_desc', dataTemp['num_fixture'].astype(str) + '-' + dataTemp['team_id'].astype(str))
dataTemp.set_index('index_desc', inplace = True)

cols = dataTemp.columns.tolist()

dataTemp_copy = dataTemp.copy(deep = True)


## ---------------------
## Opcional: Preparación de datos: estadísticas del partido anterior
## ---------------------
#data_team_statisticts_last = pd.merge(
#        dataTemp[cols[:14]].rename(columns = {'fixture_id':'fixture_id_actual'}),
#        dataTemp[cols[9:10] + cols[3:4] +  cols[5:6] + cols[14:]].rename(columns = {'fixture_id':'fixture_id_anterior','num_fixture2':'num_fixture2_y'}),
#        how = 'left',
#        left_on = ['team_id','num_fixture'],
#        right_on = ['team_id','num_fixture2_y']
#        ).drop(columns = ['num_fixture2_y'])
#
## Borro los partidos que no tuvieron información antecedente
#data_team_statisticts_last = data_team_statisticts_last[~data_team_statisticts_last.isnull().any(axis = 1)]
#
#cols = data_team_statisticts.columns.tolist()
#cols = data_team_statisticts_last.columns.tolist()
#
## Agrego datos del equipo local
#data_fixtures_past = pd.merge(
#        data_team_statisticts.iloc[:, :16],
#        data_team_statisticts_last[cols[9:10] + cols[5:6] + cols[14:]].add_suffix('.home'),
#        how = 'inner',
#        left_on = ['homeTeam.team_id','fixture_id'],
#        right_on = ['team_id.home','fixture_id_actual.home']
#        )
#
## Agrego datos del equipo visitante
#data_fixtures_past = pd.merge(
#        data_fixtures_past,
#        data_team_statisticts_last[cols[9:10] + cols[5:6] + cols[14:]].add_suffix('.away'),
#        how = 'inner',
#        left_on = ['awayTeam.team_id','fixture_id'],
#        right_on = ['team_id.away','fixture_id_actual.away']
#        )
#
#cols = data_fixtures_past.columns.tolist()
#data_ann = data_fixtures_past[cols[:16] + cols[19:246] + cols[249:]]
#cols = data_ann.columns.tolist()




# -----------------------
# Opcional: Preparación de datos: promedio de los últimos "n" partidos
# -----------------------


# Obtengo la lista de columnas a las que se calculará la media, quito las columnas de la lista listFieldsNotMean
list_columns_avg = ['team_id']
list_columns_avg.extend([x.replace('.home','') for x in list(data_team_statisticts.columns.values) if '.home' in x])


# Las siguientes líneas se requieren para calcular las columas que requieren moda
listFieldsMode = ['formation_id']

# Lista de campos con el último dato
listFieldsLast = ['type_player','rating','id_position']

# Lista de columnas que requieren el acumulado
listFieldsSum = ['is_fixtureWon','is_fixtureTied','is_fixtureLost','goalsTeam','goalsReceived','Yellow Cards','Red Cards']

# Lista de columnas que requieren la moda
list_columns_mode = ['team_id']
for e in listFieldsMode:
    list_columns_mode.extend([x.replace('.home','') for x in list(data_team_statisticts.columns.values) if ('.home' in x) & (e in x)])

# Lista de columnas que requieren el último valor
list_columns_last = ['team_id']
for e in listFieldsLast:
    list_columns_last.extend([x.replace('.home','') for x in list(data_team_statisticts.columns.values) if ('.home' in x) & (e in x)])

# Lista de columnas que requieren la suma por liga
list_columns_sum = ['team_id','league_id','is_fixtureWon','is_fixtureTied','is_fixtureLost','goalsTeam','goalsReceived']
for e in listFieldsSum:
    list_columns_sum.extend([x.replace('.home','') for x in list(data_team_statisticts.columns.values) if ('.home' in x) & (e in x)])


borrame = data_team_statisticts.columns.to_list()

# Quito las columnas con moda y últimas de la lista de columnas del promedio
for e in list_columns_mode[1:]:
    for i in list(filter(lambda x: e in x, list_columns_avg)):
        list_columns_avg.remove(i)

for e in list_columns_last[1:]:
    for i in list(filter(lambda x: e in x, list_columns_avg)):
        list_columns_avg.remove(i)

for e in list_columns_sum[2:5]: # Excluyo las columnas de tarjetas amarillas y rojas porque de ellas si requiero el promedio
    for i in list(filter(lambda x: e in x, list_columns_avg)):
        list_columns_avg.remove(i)

        
        
# Convierto todas las columnas de dataTemp a float
#dataTemp = dataTemp[list_columns_avg].astype('float32')

print ('Conversión de tipo de columnas')
for c in list_columns_avg:
    print('   Columna ' + c + ' ok')
    dataTemp[c] = dataTemp[c].astype('float32')

num_partidos_anteriores = 8

# Agrego una columna de control para saber cuales son las filas que se actualizaron
dataTemp.insert(14, 'updated', False)

print('Cálculo de medias y medianas estadísticas')

# Indexo dataTemp por numero de partido, liga y equipo
# dataTemp.drop(columns = 'index_desc', inplace = True)
dataTemp.insert(0, 'index_desc', dataTemp['num_fixture_perLeagueTeam'].astype(str) + '-' + dataTemp['league_id'].astype(str) + '-' + dataTemp['team_id'].astype(int).astype(str))
dataTemp.set_index('index_desc', inplace = True)

for d in dataTemp['num_fixture_perLeagueTeam'].sort_values().unique():
    
    print('Suma de datos para el cálculo de posicion en la tabla para partidos núm: ' + str(d))
    
    # Cálculo de la suma por liga y equipo    
    dataTemp0 = dataTemp[dataTemp['num_fixture_perLeagueTeam'] > d]
    dataTemp0 = dataTemp0.replace(-1, 0)
    dataTemp2 = dataTemp0[list_columns_sum].groupby(by=['league_id','team_id']).sum().reset_index()
    dataTemp2['team_id'] = dataTemp2['team_id'].astype(int)
    dataTemp2.set_index(d.astype(str) + '-' + dataTemp2['league_id'].astype(str) + '-' + dataTemp2['team_id'].astype(str), inplace = True)
    
    # En caso de que el número de partido ya no tenga historia:
    dataTemp0 = dataTemp[(dataTemp['num_fixture_perLeagueTeam'] == d) & (~dataTemp.index.isin(dataTemp2.index))][list_columns_sum].groupby(by = ['league_id','team_id']).count()
    dataTemp0.iloc[:,:] = 0
    dataTemp0.reset_index(inplace = True)
    dataTemp0['team_id'] = dataTemp0['team_id'].astype(int)
    dataTemp0.set_index(d.astype(str) + '-' + dataTemp0['league_id'].astype(str) + '-' + dataTemp0['team_id'].astype(str), inplace = True)

    dataTemp2 = dataTemp2.append(dataTemp0, sort = False)    
        
    dataTemp2.sort_values(by=['team_id','league_id'], inplace = True)
    dataTemp2.drop(columns=['league_id','team_id'], inplace = True)
    dataTemp2.rename(columns = {'is_fixtureWon':'num_fixturesWon',
                                'is_fixtureTied':'num_fixturesTied',
                                'is_fixtureLost':'num_fixturesLost',
                                'goalsTeam':'num_GoalsFor',
                                'goalsReceived':'num_GoalsAgainst',
                                'Yellow Cards':'num_YellowCards',
                                'Red Cards':'num_RedCards'}, inplace = True)
    dataTemp2.insert(0,'updated',True)
    
    dataTemp.update(dataTemp2)
    
    dataTemp0.reset_index(inplace = True)
    print ('   Suma: Terminado')
    


# Calculo las estadísticas del partido de acuerdo alpromedio de los "N" partidos anteriores
dataTemp.insert(0, 'index_desc', dataTemp['num_fixture'].astype(str) + '-' + dataTemp['team_id'].astype(int).astype(str))
dataTemp.set_index('index_desc', inplace = True)

for d in dataTemp['num_fixture'].sort_values().unique():
    
    print('Conversiones estadísticas de partidos núm: ' + str(d))
    
    #dataTemp2 = dataTemp[dataTemp['num_fixture'].between(d + 1, d + 3)][list_columns_avg].groupby(by = ['team_id']).mean()
    # Ahora hago la operación antenrrior en partes
    dataTemp0 = dataTemp[dataTemp['num_fixture'].between(d + 1, d + num_partidos_anteriores)]
    # Debo quitar del proceso de actualización a los equipos que tienen un -1 (esto significa que no tienen estadísticas)
    dataTemp3 = dataTemp0[['team_id','Shots insidebox']].groupby(by = ['team_id']).min().reset_index()    
    dataTemp0 = dataTemp0[dataTemp0['team_id'].isin(dataTemp3[dataTemp3['Shots insidebox'] != -1].team_id)]
    
    # Quito a los equipos que no tuvieron el número de partidos requeridos
    dataTemp3 = dataTemp0[['team_id','fixture_id']].groupby(by = ['team_id']).count().reset_index()
    dataTemp0 = dataTemp0[dataTemp0['team_id'].isin(dataTemp3[dataTemp3['fixture_id'] == num_partidos_anteriores].team_id)]
    
    # Cálculo de promedios
    dataTemp2 = dataTemp0[list_columns_avg].groupby('team_id').mean()
    dataTemp2.set_index(d.astype(str) + '-' + dataTemp2.index.astype('int32').astype(str), inplace = True)
    dataTemp2.insert(0,'updated',True)
    dataTemp.update(dataTemp2)
    print ('   Promedio: Terminado')
    
    # Cálculo de modas
    # La siguiente forma de calcular las modas eliga de entre 2 o 3 valores repetidos, el menor:
    #dataTemp2 = dataTemp0[list_columns_avg].groupby('team_id').agg(lambda x: pd.Series.mode(x)[0])
    # La siguiente forma de calcular las modas, elige el valor mayor en caso de repetidos
    dataTemp2 = dataTemp0[list_columns_mode].groupby('team_id').agg(lambda x: x.value_counts().index[0])
    dataTemp2.set_index(d.astype(str) + '-' + dataTemp2.index.astype('int32').astype(str), inplace = True)
    dataTemp2.insert(0,'updated',True)
    dataTemp.update(dataTemp2)
    print ('   Moda: Terminado')
    
    # Cálculo del último valor
    dataTemp2 = dataTemp0[list_columns_last].groupby('team_id').agg(lambda x: x.head(1))
    dataTemp2.set_index(d.astype(str) + '-' + dataTemp2.index.astype('int32').astype(str), inplace = True)
    dataTemp2.insert(0,'updated',True)
    dataTemp.update(dataTemp2)
    print ('   Último: Terminado')

    


# Modifico el índice de la tabla dataTemp y de data_ann_inut_avg para poder hacerla actualización de los datos
# El índice será: league_id-fixture
data_team_statisticts.insert(16,'updated.home',False)
data_team_statisticts.insert(17,'updated.away',False)

data_team_statisticts.insert(19, 'num_fixturesWon.home', 0)
data_team_statisticts.insert(20, 'num_fixturesTied.home', 0)
data_team_statisticts.insert(21, 'num_fixturesLost.home', 0)
data_team_statisticts.insert(22, 'num_YellowCards.home', 0)
data_team_statisticts.insert(23, 'num_RedCards.home', 0)
data_team_statisticts.insert(24, 'num_GoalsFor.home', 0)
data_team_statisticts.insert(25, 'num_GoalsAgainst.home', 0)

data_team_statisticts.insert(26, 'num_fixturesWon.away', 0)
data_team_statisticts.insert(27, 'num_fixturesTied.away', 0)
data_team_statisticts.insert(28, 'num_fixturesLost.away', 0)
data_team_statisticts.insert(29, 'num_YellowCards.away', 0)
data_team_statisticts.insert(30, 'num_RedCards.away', 0)
data_team_statisticts.insert(31, 'num_GoalsFor.away', 0)
data_team_statisticts.insert(32, 'num_GoalsAgainst.away', 0)



data_team_statisticts.sort_values(by = ['league_id','event_date','fixture_id'], ascending = [True, False, False], inplace= True)
data_team_statisticts.insert(0, 'index_desc', data_team_statisticts['league_id'].astype(str) + '-' + data_team_statisticts['fixture_id'].astype(str))
data_team_statisticts.set_index('index_desc', inplace = True)

dataTemp.sort_values(by = ['league_id','home_away','event_date','fixture_id'], ascending = [True, True, False, False], inplace= True)
dataTemp.insert(0, 'index_desc', dataTemp['league_id'].astype(str) + '-' + dataTemp['fixture_id'].astype(str))
dataTemp.set_index('index_desc', inplace = True)




# Actualizo la tabla data_ann_input_avg
# Información de locales
data_team_statisticts.update(dataTemp[dataTemp['home_away'] == 1].add_suffix('.home'))
# Información de visitantes
data_team_statisticts.update(dataTemp[dataTemp['home_away'] == 2].add_suffix('.away'))

data_team_statisticts.sort_values(by = ['league.name','event_date','fixture_id'], ascending = [True, False, False], inplace= True)

# Validación de partidos con datos
data_team_statisticts[['updated.home','updated.away','fixture_id']].groupby(by = ['updated.home','updated.away']).count().reset_index()


# Me quedo únicamente con los partidos que tienen estadísticas de los "N" partidos anteriores tanto para local como para visitante
data_team_statisticts = data_team_statisticts[(data_team_statisticts['updated.home'] == True) & (data_team_statisticts['updated.away'] == True)]


# Calculo variables de salida
data_team_statisticts.insert(15, 'goalsTotal', 0)
data_team_statisticts.loc[:, 'goalsTotal'] = data_team_statisticts[['goalsHomeTeam','goalsAwayTeam']].apply(lambda x: x['goalsHomeTeam'] if x['goalsHomeTeam'] == -1 else (x['goalsHomeTeam'] + x['goalsAwayTeam']), axis = 1)

data_team_statisticts.insert(17, 'flgIsHome', 0)
data_team_statisticts.insert(18, 'flgIsX', 0)
data_team_statisticts.insert(19, 'flgIsAway', 0)
data_team_statisticts.loc[:, 'flgIsHome'] = data_team_statisticts.apply(lambda x: 1 if x['goalsHomeTeam'] > x['goalsAwayTeam'] else 0, axis = 1)
data_team_statisticts.loc[:, 'flgIsX'] = data_team_statisticts.apply(lambda x: 0 if x['goalsHomeTeam'] == -1 else (1 if x['goalsHomeTeam'] == x['goalsAwayTeam'] else 0), axis = 1)
data_team_statisticts.loc[:, 'flgIsAway'] = data_team_statisticts.apply(lambda x: 1 if x['goalsHomeTeam'] < x['goalsAwayTeam'] else 0, axis = 1)

cols = data_team_statisticts.columns.to_list()


data_team_statisticts.to_csv('data_team_statisticts.csv')
#data_team_statisticts = pd.read_csv('data_team_statisticts.csv')
#data_team_statisticts = data_team_statisticts.iloc[:,1:]

# Validación de no repetidos
borrame = data_team_statisticts[['league_id','fixture_id','league.name']].groupby(by=['league_id','fixture_id']).count().reset_index()
borrame = borrame[borrame['league.name'] > 1]
print('Registros repetidos: ' + str(len(borrame)))



# --------------------
# Ordeno la información para que cada estadística de local tenga a la derecha la correspondiente estadística de visitante
# --------------------
cols_home = [x.replace('.home','') for x in list(data_team_statisticts.columns.values)[23:] if ('.home' in x)]
cols_home_sorted = []

for c in cols_home:
    cols_home_sorted.extend([x for x in list(data_team_statisticts.columns.values)[23:] if (c in x)])

cols = list(data_team_statisticts.columns.values)[:23] + cols_home_sorted

data_team_statisticts_sorted = data_team_statisticts[cols]


data_team_statisticts_sorted.to_csv('data_team_statisticts_sorted.csv')
#data_team_statisticts_sorted = pd.read_csv('data_team_statisticts_sorted.csv')
#data_team_statisticts_sorted = data_team_statisticts_sorted.iloc[:,1:]

borrame = dataTemp[dataTemp['team_name'] == 'Norwich']

#borrame = dataTemp_copy[dataTemp_copy['team_id'] == 548]
#borrame = dataTemp[dataTemp['team_id'] == 548].sort_values(by=['season','league_id','team_id','num_fixture'], ascending = [False, True, True, True])
#borrame2 = data_team_statisticts_sorted[(data_team_statisticts_sorted['homeTeam.team_id'] == 548) & (data_team_statisticts_sorted['fixture_id'] == 214189)].T
#borrame2 = data_team_statisticts_sorted[(data_team_statisticts_sorted['awayTeam.team_id'] == 548) & (data_team_statisticts_sorted['fixture_id'] == 214181)].T

# Validación de no repetidos
borrame = data_team_statisticts_sorted[['league_id','fixture_id','league.name']].groupby(by=['league_id','fixture_id']).count().reset_index()
borrame = borrame[borrame['league.name'] > 1]
print('Registros repetidos: ' + str(len(borrame)))


# -----------------
# RED NEURONAL ARTIFICIAL para calcular el indicador de eficiencia de juego con base en información directamente 
# relacionada al marcador de un encuentro deportivo
# -----------------
# Importar Keras y librerías adicionales
import keras
from keras.models import Sequential
from keras.layers import Dense
from keras.callbacks import EarlyStopping

from sklearn.preprocessing import LabelEncoder
from keras.utils import np_utils
from keras.wrappers.scikit_learn import KerasClassifier
from sklearn.model_selection import KFold
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import StandardScaler



data_ann = data_team_statisticts_sorted.copy(deep = True)


# Quito la información con estadísticas en -1
data_ann = data_ann[~data_ann.drop(columns = ['goalsHomeTeam','goalsAwayTeam','goalsTotal']).isin([-1]).any(axis = 1)]
data_ann['realResult'] = data_ann['realResult'].fillna('')

data_ann.insert(0, 'grupoANN', data_ann.apply(lambda x: 
    3 if (x['event_date'] >= str(datetime(2019,12,27))) else (
            1 if ((x['season'] < 2019) | ( (x['season'] == 2019) & (int(pd.to_datetime(x['event_date']).strftime('%d')) < 27)) ) else 2
        ), axis = 1))
    
# Borro los datos que no tienen realResult y que pertenecen al grupo 1 y 2 del proceso de entrenamiento
data_ann = data_ann.drop(index = data_ann[(data_ann['grupoANN'].isin([1,2])) & (data_ann['realResult'] == '')].index)
    
cols = data_ann.columns.tolist()
colsX = cols[23:]
#colsY = cols[14] # goalsTeam
colsY = cols[17] # realResult

X = data_ann[data_ann['grupoANN'].isin([1,2])].loc[:, colsX].values
Y = data_ann[data_ann['grupoANN'].isin([1,2])].loc[:, colsY].values

from sklearn.decomposition import PCA
pca = PCA(n_components=2)
X = pca.fit_transform(X)

from sklearn import preprocessing
le = preprocessing.LabelEncoder()
le.fit(Y)
le.classes_
Y_encoded = le.transform(Y)

from matplotlib import pyplot as plt
def plot_2d_space(X, y, label='Classes'):   
    colors = ['#1F77B4', '#FF7F0E', '#00AA11']
    markers = ['o', 's', 'x']
    for l, c, m in zip(np.unique(y), colors, markers):
        plt.scatter(
            X[y==l, 0],
            X[y==l, 1],
            c=c, label=l, marker=m
        )
    plt.title(label)
    plt.legend(loc='upper right')
    plt.show()


## *************
## TIPO DE MUESTRE0 
## *************
from imblearn.under_sampling import ClusterCentroids

cc = ClusterCentroids(ratio={0: 10})
X_cc, y_cc = cc.fit_sample(X, Y_encoded)

plot_2d_space(X_cc, y_cc, 'Cluster Centroids under-sampling')


## *************
## TIPO DE MUESTRE0 5
## *************
from imblearn.over_sampling import SMOTE

smote = SMOTE(ratio='minority')
X_sm, y_sm = smote.fit_sample(X, Y_encoded)

plot_2d_space(X_sm, y_sm, 'SMOTE over-sampling')



## *************
## TIPO DE MUESTRE0 6
## *************
from imblearn.combine import SMOTETomek

smt = SMOTETomek(ratio='auto')
X_smt, y_smt = smt.fit_sample(X, Y_encoded)

plot_2d_space(X_smt, y_smt, 'SMOTE + Tomek links')

from sklearn.model_selection import train_test_split
X_train_smt, X_test_smt, Y_train_smt, Y_test_smt = train_test_split(X_smt, y_smt, test_size = 0.2, random_state = 0)

Y_train_smt_oneHot = np_utils.to_categorical(Y_train_smt)
Y_test_smt_oneHot = np_utils.to_categorical(Y_test_smt)

# Reinicio índices
data_ann = data_ann.reset_index(drop = True)

cols = data_ann.columns.tolist()
colsX = cols[23:34] + cols[38:]
#colsY = cols[14] # goalsTeam
colsY = cols[17] # realResult


# --------------
# EXPLORO LA INFORMACIÓN
# --------------
X = data_ann.loc[:, colsX].values

X_train = data_ann[data_ann['grupoANN'] == 1].loc[:, colsX].values.astype(float)
X_test = data_ann[data_ann['grupoANN'] == 2].loc[:, colsX].values.astype(float)
X_new = data_ann[data_ann['grupoANN'] == 3].loc[:, colsX].values.astype(float)


## ----------
# INFORMACIÓN DE SALIDA
## ----------
print(data_ann[['realResult','fixture_id']].sort_values(by='realResult').groupby(by='realResult').count())

from sklearn import preprocessing
le = preprocessing.LabelEncoder()
le.fit(data_ann[data_ann['grupoANN'].isin([1])].loc[:, colsY].values)
le.classes_
Y_train = np_utils.to_categorical(le.transform(data_ann[data_ann['grupoANN'].isin([1])].loc[:, colsY].values))
Y_test = np_utils.to_categorical(le.transform(data_ann[data_ann['grupoANN'].isin([2])].loc[:, colsY].values))


# Create a pd.series that represents the categorical class of each one-hot encoded row
from sklearn.utils.class_weight import compute_class_weight
y_integers = np.argmax(Y_train, axis=1)
class_weights = compute_class_weight('balanced', np.unique(y_integers), y_integers)
d_class_weights = dict(enumerate(class_weights))


import tensorflow as tf
from keras.optimizers import SGD
import matplotlib.pyplot as plt
from keras.layers import Dropout
from keras.constraints import maxnorm
from sklearn.utils import class_weight
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import StratifiedKFold

# ********************
# RED. 
# ********************
classifier = tf.keras.Sequential()
classifier.add(tf.keras.layers.Dropout(0.1, input_shape=(455,)))
classifier.add(tf.keras.layers.Dense(units = 229, kernel_initializer = "uniform", activation = "tanh", input_dim = 455))
classifier.add(tf.keras.layers.Dropout(0.1))
classifier.add(tf.keras.layers.Dense(units = 3, kernel_initializer = "uniform", activation = "softmax"))

optSGD = tf.keras.optimizers.SGD(lr=0.00009)
optAdagrad = tf.keras.optimizers.Adagrad(lr=0.00009)
optAdadelta = tf.keras.optimizers.Adadelta(lr=0.00009)
optRMSprop = tf.keras.optimizers.RMSprop(lr=0.0009)
optAdam = tf.keras.optimizers.Adam(lr=0.00009)
optNadam = tf.keras.optimizers.Nadam(lr=0.00009)
optAdamax = tf.keras.optimizers.Adamax(lr=0.0009)

early_stop = EarlyStopping(monitor='val_accuracy', patience=10, restore_best_weights=True)

classifier.compile(
        optimizer = optAdam,
        loss = 'categorical_crossentropy', 
        metrics = ['accuracy'])
    
from sklearn.utils import class_weight
class_weights = class_weight.compute_class_weight('balanced',
                                                 np.unique(np.ravel(Y_train, order='C')),
                                                 np.ravel(Y_train, order='C'))

plt.plot(history.history['accuracy'])
plt.plot(history.history['val_accuracy'])
plt.ylabel('accuracy')

plt.title('model accuracy - precisión')
plt.xlabel('epoch')
plt.legend(['train', 'test'], loc='upper left')
plt.show()

# summarize history for loss
plt.plot(history.history['loss'])
plt.plot(history.history['val_loss'])
plt.title('model loss - error')
plt.ylabel('loss')
plt.xlabel('epoch')
plt.legend(['train', 'test'], loc='upper left')
plt.show()
    
# PREDICCIÓN DE DATOS NUEVOS
Y_new_pred = classifier.predict(X_new)
#Y_new_pred = sc_y_train.inverse_transform(Y_new_pred)

X_array_new_pred = pd.merge(
        pd.DataFrame(Y_new_pred, columns = ['goalsHomePred','goalsAwayPred']),
        X_array_new.reset_index(drop = True),
        left_index = True,
        right_index = True
        )


# ------------------
# Guardando los pesos de la red neuronal
# ------------------
# Serializado la red a JSON
nnClassifier_SoftmaxTanh__L06A72_json = classifier.to_json()
with open('nnClassifier_SoftmaxTanh__L06A72_json','w') as json_file:
    json_file.write(nnClassifier_SoftmaxTanh__L06A72_json)
    
# Serializar los pesos a HDF5
classifier.save_weights('classifier_SoftmaxTanh__L06A72.h5')
print('¡Modelo guardado!')


# -----------------
# USANDO REDES GUARDADAS
# -----------------
# Para usar la red: cargar json y crear el modelo
json_file = open('nnClassifier_SoftmaxTanh__L06A72_json', 'r')
loaded_model_json = json_file.read()
json_file.close()
loaded_model = tf.keras.models.model_from_json(loaded_model_json)
# cargar pesos al nuevo modelo
loaded_model.load_weights("classifier_SoftmaxTanh__L06A72.h5")
print("Cargado modelo desde disco.")

# Compilar modelo cargado y listo para usar.
opt = tf.keras.optimizers.SGD(lr=0.09)

loaded_model.compile(
        optimizer = opt, 
        loss = "mean_squared_logarithmic_error", # 'poisson'
        metrics = ['accuracy'])

Y_new_avg_pred = loaded_model.predict(X_new)
#Y_new_pred = sc_y_new.inverse_transform(Y_new_pred)

X_array_new_pred = pd.merge(
        pd.DataFrame(Y_new_avg_pred, columns = ['goalsHomePred','goalsAwayPred']),
        X_array_new.reset_index(drop = True),
        left_index = True,
        right_index = True
        )

X_array_new_pred.to_csv('X_array_new_pred.csv')
