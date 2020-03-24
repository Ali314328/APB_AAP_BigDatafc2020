#!/usr/bin/env python
# coding: utf-8

# # Seminario de Estadística
# 
# ## Tarea 0 - HDFS
# 
# ### DE: ARMANDO ARZOLA PÉREZ y ALI PINEDA BARRIOS
# 
# 

# In[4]:


get_ipython().system('pip install hdfs')
get_ipython().system('pip install snakebite')


# In[1]:


#!/usr/bin/env python
# coding: utf-8

# In[26]:


from hdfs import InsecureClient


import json


# In[27]:


def conexion(url):
    """
    url --string no null, url del host
    
    Esta función logra la conexion con hdfs 
    
    """
    try:
        client = InsecureClient(url)
        return client
    except:
        print("Ocurrio un error favor de verificar la url del host")



# In[7]:


# In[34]:


def crear_directorio(client,pathhdfs):
    """
    pathhdfs --string not null 
    Esta funcion crea un directorio en hdfs, pasandole la ruta donde sera creado
    """
    try:
        pathcreado=client.makedirs(pathhdfs)
        print("se creo el directorio " + pathhdfs)
        return pathcreado
    except:
        print("Ocurrio un error favor de verificar")



# In[35]:


def main():
    file='denuncias-victimas.json'
    url='http://localhost:9870'
    client=conexion(url)
    crear_directorio(client,"/prueba/")


# In[ ]:


if __name__=='__main__':
    main()


# 
# ## Complete las funciones restantes, documente sus funciones.
# ### 1. Cargar un archivo, elija alguno de su directorio de preferencia un json o un txt.
# ### 2. Listar archivos dentro de una ruta de hdfs
# ### 3. Leer un archivo de hdfs.
# ### 4. Elimine el directorio
# 
# 

# In[11]:


# In[40]:
#1

def cargar_archivo(client,pathhdfs,local_path):
    from snakebite.client import Client
    client=Client('localhost',9870)
    for i in client.put(['denuncias-victimas.json'],'/pathhdfs'):
        print(i)



# In[4]:


# In[41]:



def lista_directorio(client,path):
    client = Client('localhost', 9870)
    for i in client.ls(['/pathhdfs']):
        print(i)
        



# In[12]:


# In[42]:


def lectura_HDFS(cliente,path_archivo):
    client = Client('localhost', 9870)
    for i in client.text(['/input/denuncias-victimas.json']):
           print (i)


# In[13]:



# In[43]:


def eliminar_directorio(client,path_hdfs):
    client = Client('localhost', 9870)
    for i in client.delete(['/pathhdfs', '/input'], recurse=True):
        print(i)

