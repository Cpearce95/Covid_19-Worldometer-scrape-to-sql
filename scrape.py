import pandas as pd,numpy as np, requests, sqlalchemy as sa, os, datetime, time 
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from datetime import *

url = r"https://www.worldometers.info/coronavirus/"
r = requests.get(url)
soup = BeautifulSoup(r.text, 'html.parser')
places,total_cases,new_cases,total_deaths,new_deaths,total_recovered,population,datetimes = [],[],[],[],[],[],[],[]
def scrape():
    for tr in soup.find_all('tr'):
        if len(tr.find_all('td')) > 15:
            place = tr.find_all('td')[1].text
            total_case = tr.find_all('td')[2].text
            new_case = tr.find_all('td')[3].text
            total_death = tr.find_all('td')[4].text
            new_death = tr.find_all('td')[5].text
            total_recover = tr.find_all('td')[6].text
            pop = tr.find_all('td')[14]
            places.append(place)
            total_cases.append(total_case)
            new_cases.append(new_case)
            total_deaths.append(total_death)
            new_deaths.append(new_death)
            total_recovered.append(total_recover)
            population.append(pop.text if len(pop.text) > 2 else 'N/A')
            datetimes.append(str(datetime.now()))

scrape()
dict = {'Location': places,'Total_Cases': total_cases,'New_Cases': new_cases,'Total_Deaths': total_deaths,'New_Deaths': new_deaths,'Total_Recovered': total_recovered,'Population':population,'Datetime':datetimes}
covid_df2 = pd.DataFrame(dict)
covid_df2 = covid_df2.applymap(lambda x: x.replace('+', '')).applymap(lambda x: x.replace(',','')).applymap(lambda x: x.replace(':','')).applymap(lambda x: x.strip())
covid_df2 = covid_df2.replace('',np.NaN)
covid_df2 = covid_df2.replace('N/A',np.NaN)
covid_df2 = covid_df2.fillna(0)
covid_df2['Total_Cases'] = covid_df2['Total_Cases'].astype('int64')
covid_df2['New_Cases'] = covid_df2['New_Cases'].astype('int64')
covid_df2['Total_Deaths'] = covid_df2['Total_Deaths'].astype('int64')
covid_df2['New_Deaths'] = covid_df2['New_Deaths'].astype('int64')
covid_df2['Total_Recovered'] = covid_df2['Total_Recovered'].astype('int64')
covid_df2['Population'] = covid_df2['Population'].astype('int64')
covid_df2['Datetime'] = covid_df2['Datetime'].astype('datetime64')
covid_df2 = covid_df2.drop_duplicates(subset='Location')
covid_df2 = covid_df2.drop(covid_df2.index[[0,1,2,3,4,5,6]]).reset_index(drop=True)
server,db = 'SERVER_NAME','covid_19_db'
engine = sa.create_engine(f'mssql+pyodbc://{server}/{db}?Trusted_Connection=yes&Driver=ODBC Driver 17 for SQL Server')
con = engine.connect()
covid_df2.to_sql('tbl_covid_worldometer',if_exists='append',index=None,con=con)
