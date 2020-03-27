#!/usr/bin/env python
# coding: utf-8

# In[26]:


from hdfs import InsecureClient
import json


# In[27]:


def conexion(url,user):
    """
    url --string no null, url del host
    
    Esta función logra la conexion con hdfs 
    
    """
    try:
        client = InsecureClient(url,user)
        return client
    except:
        print("Ocurrio un error favor de verificar la url del host")


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


# Complete las funciones restantes, documente sus funciones.
# 1. Cargar un archivo, elija alguno de su directorio de preferencia un json o un txt.
# 2. Listar archivos dentro de una ruta de hdfs
# 3. Leer un archivo de hdfs.
# 4. Elimine el directorio

# In[40]:


def cargar_archivo(client,pathhdfs,local_path):
    """
        Esta funcion carga el archivo
        desde el directorio local_path
        hacia el directorio pathhdfs
    """
    try:
        upload_path=client.upload(hdfs_path=pathhdfs,local_path=local_path)
        print("se cargo el archivo en la ruta de carga  " + upload_path)
        return upload_path
    except:
        print("Ocurrio un error favor de verificar las rutas local del archivo y ruta objetivo")
    

# In[41]:


def lista_directorio(client,path):
    """
    imprime la lista de los directorios en la ruta indicada
    esto mediante el metodo lisy()
    """
    try:
        lista=client.list(path)
        print(lista)
        return lista 
    except:
        print("Ocurrio un error, favor de verificar la ruta hdfs")


# In[42]:


def lectura_HDFS(client,path_archivo):
    """
    Lee el archivo indicado en path_archivo
    este desde hdfs, se debe usar el with
    """
    try:
        with client.read(path_archivo) as reader:
            contenido=reader.read()
            print("La lectura del archido es: " + contenido)
            return contenido
    except:
        print("Error en la ruta del archivo")

# In[43]:


def eliminar_directorio(client,path_hdfs):
    """
    elimina el directorio indicado en path_hdfs
    regresa un true o false, indicando el estatus del directorio
    """
    try:
        estatus=client.delete(path_hdfs)
        print("Se elimino el directorio" + path_hdfs)
        return estatus
    except:
        print("Error en la ruta del directorio a eliminar")

# In[35]:


def main():
    file='archivo.txt'
    user="hdfs"
    url='http://localhost:50070'
    client=conexion(url,user)
    pathhdfs="/prueba"
    crear_directorio(client,pathhdfs)
    lista_directorio(client,pathhdfs)
    local="/Users/Jose Damian/Desktop/Tareas/Seminario_Estadistica"
    cargar_archivo(client,pathhdfs,local)
    eliminar_directorio(client,pathhdfs)
# In[ ]:


if __name__=='__main__':
    main()

