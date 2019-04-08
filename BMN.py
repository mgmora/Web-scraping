'''
Created on 4 abr. 2019

@author: Youness & Mario
'''
# Importamos librerias necesarias
import requests
from bs4 import BeautifulSoup
import time
import datetime as dt
import re
import pandas as pd
from requests import get

class BMNdata():
    
    # Constants
    URL_BASE = "http://www.bolsamadrid.es/esp/aspx/Indices/Resumen.aspx"
    DELAY = 10
    TIMEOUT = 20
    
    def __init__(self):
        """
        Iniciliza la clase BMNdata a partir de una lista de variables objetivo
        ejecutando el webscrapping de los datos en bolsamadrid.es
        """
        # inicializamos la variable interna tlast (ultima request, solicitada)
        self._tlast = time.clock()
        
        # Creamos el nombre del fichero csv
        self._filename = 'data/BMN_.csv'
        
            
        # Ejecutamos web scraping
        ret = self.__execute()
        
        # Creamos la soup
        html_soup = BeautifulSoup(ret.text, 'html.parser')
        
        indicesTable = html_soup.find('table', class_="TblPort")
        
        
        df = self.__getDataframe(indicesTable)
        
        df.to_csv(self._filename)
    
    
        
    def __getDataframe(self, indicesTable):
        """
        Devuelve un dataframe de pandas a partir de una tabla de datos en formato
        de texto plano con separador de ;
        
        Es el tipo de formato de origen 2
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
        
        
        # Inicializamos el df
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
        """
        Ejecuta el WebScraping
        """
        
         # Configuramos la url destino
        #url = self.URL_BASE + var + '/'
        url = self.URL_BASE
        
        try:
            # Realizamos la request con 20s de timeout y headers personalizados
           # headers = ({'User-Agent':
            #'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'})
            
            #responseI = get(url, headers=headers)
            
            #html_soup = BeautifulSoup(responseI.text, 'html.parser')
            #fixed_htmlI = html_soup.prettify()
            html = requests.get(url, timeout = self.TIMEOUT)
            
            #  Actualizamos el tiempo de ultima request solicitada a la web
            self._tlast = time.clock()
            
            # Comprobamos que el status de la request sea 200 y devolvemos con warning si no es así
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
