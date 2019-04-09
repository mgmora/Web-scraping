'''
Created on 4 abr. 2019

@author: Youness & Mario
'''
# Import Libraries
import requests
from bs4 import BeautifulSoup
import time
import datetime as dt
import re
import pandas as pd
from requests import get
import datetime

class BMNdata():
    
    # Constants
    URL_BASE = "http://www.bolsamadrid.es/esp/aspx/Indices/Resumen.aspx"
    URL_BASE_ROBOT = "http://www.bolsamadrid.es/robot.txt"
    DELAY = 10
    TIMEOUT = 20
    
    def __init__(self, start_date='2019-04-09', end_date='2019-05-01'):
        """
        Iniciliza la clase BMNdata a partir de una lista de variables objetivo
        ejecutando el webscrapping de los datos en bolsamadrid.es
        """
        # Timeout init 
        self._tlast = time.clock()
        #date = datetime.date.today().strftime("%Y_%B_%d_%I_%M_%p")
        #date = date.replace(",", "_")
        
        # Create csv output file
        self._filename = 'data/BMN_' + ''.join(start_date.split('-')) + '_' + ''.join(end_date.split('-')) + '.csv'
        #self._filename = r"C:\Users\8460P\Desktop\Master\TIPOLOGIA Y CICLO DE VIDA DATOS\BMN_.csv"
        
         # Inicializamos fechas como datetime
        self.start_date = dt.datetime.strptime(str(start_date), "%Y-%m-%d")
        self.end_date = dt.datetime.strptime(str(end_date), "%Y-%m-%d")
        
        # Si la fecha de inicio es mayor que la fecha fin las intercambiamos
        if self.start_date > self.end_date: self.start_date, self.end_date = self.end_date, self.start_date
        
        # Check robot.txt file
        if self.does_url_exist(self.URL_BASE_ROBOT) == False:
            #launch webScraping
            self.__execute()
        
    #check if exists robot.txt    
    def does_url_exist(self, url):
        try: 
            r = requests.head(url)
            if r.status_code < 400:
                return True
            else:
                print("Robot file don t exist {}.".format(url))
                return False
        except requests.exceptions.RequestException as e:
            print(e)
        
        
    def __getDataframe(self, indicesTable):
        """
        Return dataFrame with data
        """
        
         # setting up the lists that will form our dataframe with all the results
        nombres = []
        anteriores = []
        ultimos = []
        difs = []
        maximos = []
        minimos = []
        fechas = []
        horas = []
        
        if indicesTable != []:
            
            indices = indicesTable.findChildren("tr" , recursive=False)
            
            #skip first row
            iterindex = iter(indices)
            next(iterindex)
                    
            for indice in iterindex:
    
                data = indice.find_all("td")
            
                # Nombres            
                nombre = data[0].text
                nombres.append(nombre)
                
                # Anteriores
                anterior = data[1].text
                anteriores.append(anterior)

                # Ultimos
                ultimo = data[2].text
                ultimos.append(ultimo)

                # Difs
                dif = data[3].text
                difs.append(dif)

                # Maximos
                maximo = data[4].text
                maximos.append(maximo)

                # Minimos
                minimo = data[5].text
                minimos.append(minimo)

                # Fechas
                fecha = data[6].text
                fechas.append(fecha)

                # Horas
                hora = data[7].text
                horas.append(hora)
        
        cols = ['Nombres', 'Anteriores', 'Ultimos', 'Difs', 'Maximos', 'Minimos', 'Fechas', 'Horas']
        
        
        # Init dataframe
        df = pd.DataFrame({'Nombres': nombres,
                           'Anteriores': anteriores,
                           'Ultimos': ultimos,
                           'Difs': difs,
                           'Maximos': maximos,
                           'Minimos': minimos,
                           'Fechas': fechas,
                           'Horas': horas})[cols]
        
        return df
    
    def __execute(self):
        
        # Launch Webscraping
        ret = self.__launchWebScraping()
        
        # Create soup
        html_soup = BeautifulSoup(ret.text, 'html.parser')
        
        # Get interest table
        indicesTable = html_soup.find('table', class_="TblPort")
        
        # Parse xml to dataFrame
        df = self.__getDataframe(indicesTable)
        
        # Save dataFrame into file
        print("Creating " + self._filename + " file..." )
        df.to_csv(self._filename)

    def __launchWebScraping(self):
        """
        Launching WebScraping with spicific agent and timeout
        """
        
        # Settinh url
        url = self.URL_BASE
        
        try:
            # launche request for spicific timeout
            headers = ({'User-Agent':
            'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'})
            
            html =  requests.get(url, headers=headers, timeout = self.TIMEOUT)
            
            #  Set last time with current time
            self._tlast = time.clock()
            
            # Check request status to detect connection failed
            if html.status_code != 200:
                print("STATUS CODE {} on {}. Check it out.".format(html.status_code,url))
            else:
                print("ALL GOOD for {}".format(url))
            return html
        
        except requests.exceptions.Timeout:
            print("TIMEOUT on {}.".format(url))
            tries = 0
            # wait and retry it
            if tries < 2:
                time.sleep(10)
                self.__execute()
                tries += 1
    
        except requests.exceptions.RequestException:
            print("Connection Failed on {}.".format(url))
            tries = 0
            # wait and retry it
            if tries < 2:
                time.sleep(10)
                self.__execute()
                tries += 1
