# Beer-Data
Big Data project about beers around the world

# Links
[Página web del proyecto]()

# Datos
* [Dataset de Kaggle](https://www.kaggle.com/datasets/volodymyrpivoshenko/multi-aspect-beer-reviews)
Dataset con más de millón y medio de reviews entre los años 1998-2011 con cervezas de todo el mundo en donde se encuentra una descripción detallada de cada cerveza con caracteírsticas fundamentales como el ABV, sabor, apariencia, aroma...
* [Dataset completo alojado en Drive](https://drive.google.com/file/d/1vjSE_9jBK57TYwqUhIQ2zHbTSk7UfHPo/view)
Dataset con el original obtenido de Kaggle y otro creado por nosotro, el script se encuentra alojado en la carpeta **code** llamado [Crear_dataset.py](https://github.com/ROGOSE/Beer-Data/blob/main/code/Crear_dataset.py) en donde cogiendo todos los aspectos específicos a los que se da una nota de 0-5 (sabor, apariencia, sensación, aroma, impresión general) se les aplican los siguientes porcentajes:
    * 6% Apariencia
    * 24% Aroma
    * 40% Sabor
    * 10% Sensación
    * 20% Impresión general

  # Pasos previos ejecución
  Para poder procesar los scripts que hemos desarrollado en un equipo Ubuntu o en una instancia de Google Cloud, antes deben seguir los siguientes pasos:
  ## Equipo Ubuntu
1. **Instalación de Java**<br />
 ```
 $ sudo apt install default-jre
 $ java -version
 ```
2. **Instalación de Python**
```
$ sudo apt-get update
$ sudo apt-get install python3.6
```
3. **Instalación de Spark**
```
$ curl -O https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
$ tar xvf spark-3.3.1-bin-hadoop3.tgz
$ sudo mv spark-3.3.1-bin-hadoop3 /usr/local/spark
```
4. **Configuración del entorno** <br />
Se añade /usr/local/spark/bin al PATH en el fichero ~/.profile. Después de actualiza el PATH en la sesión actual.
```
$ echo 'PATH="$PATH:/usr/local/spark/bin"' >> ~/.profile
$ source ~/.profile
```
5. **Instalación de librerias**
```
$ sudo apt install python3-pip
$ pip install wordcloud
$ sudo apt-get install python3-matplotlib
