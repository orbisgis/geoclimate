Frequently asked questions
==========================

GeoClimate reports “java.lang.OutOfMemoryError: Java heap space”
----------------------------------------------------------------

As the error message suggests, you have run out of memory on your
GeoClimate instance. Increase it with the following arguments :

::

   java -Xmx1024m -jar geoclimate.jar -f myconfigFile.json

See
https://docs.oracle.com/cd/E15523_01/web.1111/e13814/jvm_tuning.htm#PERFM160

How to increase the size of the data downloaded from OSM ?
----------------------------------------------------------

In the “input” section of your configuration file, you need to add the
“timeout”, “maxsize” and “area” keywords and the corresponding value of
your choice:

.. code:: json

   "input" : [
                           "locations" : ["Pont-de-Veyle"],
                           "timeout" : 900,
                           "maxsize":1073741824,
                           "area": 1000
   ]

For more information about “timeout” and “maxsize” keywords, have a look
`here `__ and for
“area” look `here `__.

How to force the SRID with the BDTopo workflow ?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sometimes the prj file does not contain any information about the EPSG
code as defined here https://www.ogc.org/standards/wkt-crs. If the input
files set to GeoClimate doesn’t have a SRID then GeoClimate abort the
process. To fix this problem you can either modify the prj or set a srid
parameter as below. SRID value will be forced for all input geometries.
Make sure you are in the right coordinate reference system before using
this parameter.

.. code:: json

   "input" : [
                           "locations" : ["Pont-de-Veyle"],
                           "srid" : 2154,
   ]

How to use GeoClimate with proxy configuration ?
------------------------------------------------

Depending on networking environments, particularly corporate ones, you
must have to deal with proxy configuration.

If you run Geoclimate with a Groovy script, tune the proxy just like
that

.. code:: java

   System.getProperties().put("proxySet", true);
   System.getProperties().put("proxyHost", "proxyUrl");
   System.getProperties().put("proxyPort", "proxyPort");

If you use the Geoclimate CLI try this :

.. code:: java

   java -Djava.net.useSystemProxies=true -Dhttp.proxyHost=10.10.10.10 -Dhttp.proxyPort=8080  -jar  Geoclimate.jar -f osm_geoclimate.json

if nothing works, please contact your system administrator ;-)

Groovy version issue
--------------------

Using the groovyConsole, you may meet this error: > Exception thrown
groovy.lang.GroovyRuntimeException: Conflicting module versions. Module
[groovy-sql is loaded in version 3.0.14 and you are trying to load
version 3.0.11

What groovy tries to tell you is that you should use groovy 3.0.11. To
solve that we recommend: 1. Download sdk 2. display the list of groovy
installed versions: > sdk list groovy 3. if the default one is not the
one you want, you need to do: > sdk default groovy 3.0.11 (for example)
4. if this version is not install you will have to install it: > sdk
install groovy 3.0.11

If you want to install a version not directly dowloadable (for example
3.0.0-rc-2):
http://apache.crihan.fr/dist/groovy/3.0.0-rc-2/sources/apache-groovy-src-3.0.0-rc-2.zip

Then in a terminal, you point the folder where you have unzip your
groovy version and you give him an alias (e.g. 3.0.0-rc-2SNAP): sdk
install groovy 3.0.0-rc-2SNAP /home/decide/Software/groovy-3.0.0-rc-2

How to custom log messages with GeoClimate CLI ?
------------------------------------------------

GeoClimate CLI introduces a -l command to custom the messages printed by
GeoClimate.

.. code:: java

   //Shows info messages
   java  -jar geoclimate.jar -f myconfigFile.json -l info

   //Shows debug messages
   java  -jar geoclimate.jar -f myconfigFile.json -l debug

   //Shows trace messages
   java  -jar geoclimate.jar -f myconfigFile.json -l trace

   //Turn off all messages
   java  -jar geoclimate.jar -f myconfigFile.json -l off

Note that the log level info, debug, trace, off is applied for all
loggers loaded by GeoClimate including third party libraries.

The default log level is set to info.

How to compile the GeoClimate wiki documentation in PDF ?
---------------------------------------------------------

Install the Github Wikito Converter avaible at
https://github.com/yakivmospan/github-wikito-converter

::

   # Clone the wiki documentation
    git clone https://github.com/geoclimate/geoclimate.wiki.git

::

   # Convert the geoclimate.wiki folder
    gwtc -t "GeoClimate 1.0.0 documentation" --pdf-page-count --footer "GeoClimate - documentation 2020 - 2024 - Creative Commons Attribution-ShareAlike 4.0 International. " -f pdf ./geoclimate.wiki/ -n GeoClimate_1.0.0_documentation

Combine the resulting pdf with the `pdf first
page `__
(first updating versions and dates).

.. include:: _Footer.rst
