#!/usr/bin/env python
# coding: utf-8

# In[1]:


# import libraries
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime


# In[2]:


# indicar la ruta
url_page = 'http://www.bolsamadrid.es/esp/aspx/Indices/Resumen.aspx'


# In[3]:


# tarda 480 milisegundos
page = requests.get(url_page).text 
soup = BeautifulSoup(page, "lxml")


# In[4]:


# Obtenemos la tabla por un ID específico
tabla = soup.find('table', attrs={'id': 'ctl00_Contenido_tblÍndices'})
tabla


# In[5]:


name=""
price=""
nroFila=0
for fila in tabla.find_all("tr"):
    if nroFila==1:
        nroCelda=0
        for celda in fila.find_all('td'):
            if nroCelda==0:
                name=celda.text
                print("Indice:", name)
            if nroCelda==2:
                price=celda.text
                print("Valor:", price)
            nroCelda=nroCelda+1
    nroFila=nroFila+1


# In[8]:


# Abrimos el csv con append para que pueda agregar contenidos al final del archivo
with open('bolsa_ibex35.csv', 'a') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow([name, price, datetime.now()])


# In[ ]:




