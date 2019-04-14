'''
Created on 4 abr. 2019

@author: Youness & Mario
'''
# Import Libraries
import requests
import os
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
    
    def __init__(self, start_date='2019-04-14', end_date='2019-04-15'):
        """
        Iniciliza la clase BMNdata a partir de una lista de variables objetivo
        ejecutando el webscrapping de los datos en bolsamadrid.es
        """
        # Timeout init 
        self._tnow = 0
        self._tstart_dat = ""
        self._tstart_hor= ""
        self._tlast_dat = ""
        self._tlast_hor= ""
        # Get current date
        self._current = datetime.datetime.now()
        
        # Create csv output file
        self._filename = 'data/BMN_' + ''.join(start_date.split('-')) + '_' + ''.join(end_date.split('-')) + '.csv'
        
         # Init dates as timeStamp
        self.start_date = dt.datetime.strptime(str(start_date), "%Y-%m-%d")
        self.end_date = dt.datetime.strptime(str(end_date), "%Y-%m-%d")
        
        # If start_date > end_date swap 
        if self.start_date > self.end_date: self.start_date, self.end_date = self.end_date, self.start_date
        
        if self._current < self.start_date or self._current > self.end_date:
            print("Current date:" + str(self._current) +" should be between Start date:" + str(self.start_date) + " and End date:" + str(self.end_date))
            print("Please change start date and end date parameters !!!")
        while self._current >= self.start_date and self._current <= self.end_date:
        
            # Check robot.txt file
            if self.does_url_exist(self.URL_BASE_ROBOT) == False:
                #launch webScraping
                self.__execute()
            
            #Sleep for TIMEOUT
            time.sleep(self.TIMEOUT) 
            
            # Get current date
            self._current = datetime.datetime.now()
        
            
    def does_url_exist(self, url):
        
        #set tlast to avoid block
        self._tlast = time.clock()
        
        # check delay 
        if self._tnow > 0 and (self._tnow-self._tlast) < self.DELAY: 
            time.sleep(self.DELAY - (self._tnow-self._tlast))  
        
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
        Difs_Anno_2019 = []
        
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
                
                # Diferencia con año 2019
                Dif_Año_2019 = data[8].text
                Difs_Anno_2019.append(Dif_Año_2019)
        
        cols = ['Nombres', 'Anteriores', 'Ultimos', 'Difs', 'Maximos', 'Minimos', 'Fechas', 'Horas', 'Difs_Anno_2019']
        
        
        # Init dataframe
        df = pd.DataFrame({'Nombres': nombres,
                           'Anteriores': anteriores,
                           'Ultimos': ultimos,
                           'Difs': difs,
                           'Maximos': maximos,
                           'Minimos': minimos,
                           'Fechas': fechas,
                           'Horas': horas,
                           'Difs_Anno_2019': Difs_Anno_2019})[cols]
        
        self._tlast_dat = fecha
        self._tlast_hor= hora
        
        return df
    
    def __execute(self):
        
        #Get now time
        self._tnow = time.clock()
        # Launch Webscraping
        ret = self.__launchWebScraping()
        
        # Create soup
        html_soup = BeautifulSoup(ret.text, 'html.parser')
        
        # Get interest table
        indicesTable = html_soup.find('table', class_="TblPort")
        
        # Parse xml to dataFrame
        df = self.__getDataframe(indicesTable)
        
        #Check if data changed
        if self._tstart_dat != self._tlast_dat  or self._tlast_hor != self._tstart_hor:
            # append if already exists
            if os.path.exists(self._filename):
                print("Appending dato to " + self._filename + " ..." )
                df.to_csv(self._filename, mode='a', header=False)
            # make a new file if not
            else:
                print("Creating " + self._filename + " file..." )
                df.to_csv(self._filename) 
            self._tstart_dat = self._tlast_dat
            self._tstart_hor = self._tlast_hor  
        else: 
            print("No updates was found ..." ) 

    def __launchWebScraping(self):
        """
        Launching WebScraping with spicific agent and timeout
        """
        
        # Settinh url
        url = self.URL_BASE
        
        self._tnow = time.clock()
        
        # check delay 
        if (self._tnow-self._tlast) < self.DELAY: 
            time.sleep(self.DELAY - (self._tnow-self._tlast))            
        
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
                time.sleep(self.TIMEOUT)
                self.__execute()
                tries += 1
    
        except requests.exceptions.RequestException:
            print("Connection Failed on {}.".format(url))
            tries = 0
            # wait and retry it
            if tries < 2:
                time.sleep(self.TIMEOUT)
                self.__execute()
                tries += 1
