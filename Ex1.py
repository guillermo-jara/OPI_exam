# -*- coding: utf-8 -*-
"""
Created on Sun Jun 13 22:36:15 2021

@author: enano
"""


import pandas as pd


df=pd.read_csv('all_data.csv')

print("1. El total número de registros de la base es de: "+ str(df.shape[0]) + ".\n")

print("2. Existen un total de " + str(df['categoria'].nunique()) + " categorias en la base.\n")

print("3. Existen un total de " + str(df['cadenaComercial'].nunique()) + " cadenas comerciales en la base.\n")

res4=df.groupby(['estado', 'producto'])['marca'].count()
res4=res4.reset_index()
res4=res4[res4['estado']!='estado']
res4.columns
res4=res4.sort_values(['estado', 'marca', 'producto'], ascending=False)

print("4. Los productos más monitoreados por cada estado de la república son: \n")
print(res4.groupby('estado').head(1)['producto'])

res5=df.groupby(['cadenaComercial', 'producto'])['marca'].count().reset_index()

print("5. La cadena comercial con mayor cantidad de productos monitoreados es "+
      res5.groupby('cadenaComercial')['producto'].nunique().sort_values(ascending=False).head(1).index[0] + 
      " con un total de " + str(res5.groupby('cadenaComercial')['producto'].nunique().sort_values(ascending=False).head(1)[0]) 
      " productos.\n" )




