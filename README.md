# Beer-Data
Proyecto final de BigData basado en el análisis y estudio de cervezas de todo el mundo con más de millón y medio de reseñas sobre más de 50.000 cervezas diferentes.

# Miembros del grupo
   * Alberto Gómez de Andrés
   * Rodrigo Gómez Serrano
   * Gabriel Casado Valcárcel
   * Javier Gómez Arribas 

# Links
[Página web del proyecto](https://rogose.github.io/Beer-Data/)

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
```
$ echo 'PATH="$PATH:/usr/local/spark/bin"' >> ~/.profile
$ source ~/.profile
```
5. **Instalación de librerias**
```
$ sudo apt install python3-pip
```
6. **Comprobación de buena instalación**
```
$ spark-submit /usr/local/spark/examples/src/main/python/pi.py 10
```
Si todo se ha instalado correctamente debería salirnos una línea diciendo : Pi is roughly 3.142480  <br />
Una vez la instalación se ha efectuado correctamente, ya podemos ejecutar los scripts del proyecto usando el siguiente comando:
```
$ spark-submit <nombre_fichero.py> <nombre_dataset.json> <directorio_output>
```

## Instancia en Google Cloud
1. **Creación de Cluster**  <br />
En la consola de Cloud:
```
$ gcloud dataproc clusters create example-cluster --region europe-west6 --master-boot-disk-size 50GB --worker-boot-disk-size 50GB --enable-component-gateway
```
2. **Creación Bucket** <br />
   1. Ve a Navigation menu () > Cloud Storage > Buckets.
   2. Click CREATE.
   3. Rellena la información del bucket information y click CONTINUE para completar los siguientes pasos:
         * Dar nombre al bucket: debe ser único.
         * Elegir para <strong>Location type</strong>: Region y para <strong>Location</strong>: europe-west6 (Zurich).
         * Elegir para <strong>Default Storage Class</strong>: Standard.
         * Habilite <strong>Enforce public access prevention on this bucket</strong> y elija <strong>Uniform for Access control</strong>.
         * Elija <strong>None for Protection tools</strong>.
   4. Click CREATE

3. **Una vez ya creado el Cluster y el Bucket hay dos formas de ejecutar el código:**
* Mediante envios de trabajos desde Cloud Shell:
  ```
  $ BUCKET=gs://$BEER_DATA_PROJECT
  $ gcloud dataproc jobs submit pyspark --cluster example-cluster --region=europe-west6 $BUCKET/<nombre_fichero.py> -- $BUCKET/<nombre_dataset.json> $BUCKET/<nombre_output>
  ```
* Mediantes envios de trabajo desde Cluster's Master Node:
  Ir a <strong>Dataproc</strong> > <strong>Cluster</strong> > <strong>Cluster_info</strong> > <strong>Virtual Machines</strong> y entrar en master usando SSH:
  ```
  $ BUCKET=gs://$GOOGLE_CLOUD_PROJECT
  $ spark-submit $BUCKET/wordcount.py $BUCKET/input $BUCKET/output3
  ```
  Se puede cambiar el número de <strong><em>executors</em></strong> y <strong><em>cores</em></strong> con los que queremos trabajar:
  ```
  $ spark-submit --num-executors <n_executors> --executor-cores <n_cores> filter_cloud.py
  ```

# Descripción de código:
Se han desarrollado una serie de scripts en lenguaje Python utilizando Apache Spark como framework de computación de los cuales describiremos su funcionalidad a continucación:
  * [Cervezas_más_populares.py](https://github.com/ROGOSE/Beer-Data/blob/main/code/Cervezas_m%C3%A1s_populares.py): se obtiene un csv con una lista ordenada de forma decreciente con las cervezas más populares (las que tienen más reseñas), con el número de reviews que tiene, con la nota media de la cerveza, así como la nota de su mejor y peor review y su corespondiente comentario. Los csv's resultantes se puede unificr todo en uno, o crear dos uno con las mejores notas y sus reviews y otro con las peores. El dataset que se debe usar es <strong>beeradvocate_note.json</strong>.
  * [Topear_cervezas_por_rango.py](https://github.com/ROGOSE/Beer-Data/blob/main/code/Topear_cervezas_por_rangos.py): se obtiene un csv con un listado con las cervezas ordenadas por su nota media y agrupadas por rangos de la nota media, los rangos son:
      * Rango 1: 0 < nota media <= 1
      * Rango 2: 1 < nota media <= 2
      * Rango 3: 2 < nota media <= 3
      * Rango 4: 3 < nota media <= 4
      * Rango 5: 4 < nota media <= 5
       El dataset que se debe usar es <strong>beeradvocate_note.json</strong>.
  * [Mejores_cervezas_por_año](https://github.com/ROGOSE/Beer-Data/blob/main/code/Mejores_cervezas_por_a%C3%B1o.py): se obtiene un csv con una lista en la que por cada año se saca un ranking de las 5 mejores cervezas valoradas ese año, con su nota media y el nombre de la cerveza.  El dataset que se debe usar es <strong>beeradvocate_note.json</strong>.
  * [Topear_cerveceras_con_cervezas](https://github.com/ROGOSE/Beer-Data/blob/main/code/Topear_cerveceras_con_cervezas.py): se obtiene un csv con el listado de todas las cerveceras que hay en el dataset ordenado por su nota media que se ha obtenido sacando la nota media de todas las cervezas que ha producido ese cervecera, y uan vez obtenido todas las medias volver a hacer la media con todas ellas. Y además al se incluye una columan con el nombre de todas las cervezas creadas por dicha cervecera.  El dataset que se debe usar es <strong>beeradvocate_note.json</strong>.
  
  
