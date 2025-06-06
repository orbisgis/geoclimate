Frequently asked questions
==========================

GeoClimate reports "java.lang.OutOfMemoryError: Java heap space"
----------------------------------------------------------------

As the error message suggests, you have run out of memory on your
GeoClimate instance. Increase it with the following arguments:

::

   java -Xmx1024m -jar geoclimate.jar -f myconfigFile.json

See `Java VM Tuning <https://docs.oracle.com/cd/E15523_01/web.1111/e13814/jvm_tuning.htm#PERFM160>`_.

How to increase the size of the data downloaded from OSM?
---------------------------------------------------------

In the "input" section of your configuration file, you need to add the
"timeout", "maxsize", and "area" keywords and the corresponding value of
your choice:

.. code:: json

   {
     "input": {
       "locations": ["Pont-de-Veyle"],
       "timeout": 900,
       "maxsize": 1073741824,
       "area": 1000
     }
   }

For more information about "timeout" and "maxsize" keywords, have a look
`here for timeout and maxsize <https://example.com/timeout_maxsize>`_ and for
"area" look `here for area <https://example.com/area>`_.

How to force the SRID with the BDTopo workflow?
-----------------------------------------------

Sometimes the prj file does not contain any information about the EPSG
code as defined `in the WKT-CRS standard <https://www.ogc.org/standards/wkt-crs>`_. If the input
files set to GeoClimate don't have a SRID, then GeoClimate aborts the
process. To fix this problem, you can either modify the prj or set a srid
parameter as below. The SRID value will be forced for all input geometries.
Make sure you are in the right coordinate reference system before using
this parameter.

.. code:: json

   {
     "input": {
       "locations": ["Pont-de-Veyle"],
       "srid": 2154
     }
   }

How to use GeoClimate with proxy configuration?
-----------------------------------------------

Depending on networking environments, particularly corporate ones, you
must have to deal with proxy configuration.

If you run GeoClimate with a Groovy script, tune the proxy like this:

.. code:: java

   System.getProperties().put("proxySet", "true");
   System.getProperties().put("proxyHost", "proxyUrl");
   System.getProperties().put("proxyPort", "proxyPort");

If you use the GeoClimate CLI, try this:

.. code:: java

   java -Djava.net.useSystemProxies=true -Dhttp.proxyHost=10.10.10.10 -Dhttp.proxyPort=8080 -jar Geoclimate.jar -f osm_geoclimate.json

If nothing works, please contact your system administrator.

Groovy version issue
--------------------

Using the groovyConsole, you may encounter this error: > Exception thrown
groovy.lang.GroovyRuntimeException: Conflicting module versions. Module
[groovy-sql] is loaded in version 3.0.14 and you are trying to load
version 3.0.11.

What Groovy tries to tell you is that you should use Groovy 3.0.11. To
solve that, we recommend:

1. Download SDK.
2. Display the list of Groovy installed versions: > sdk list groovy.
3. If the default one is not the one you want, you need to do: > sdk default groovy 3.0.11 (for example).
4. If this version is not installed, you will have to install it: > sdk install groovy 3.0.11.

If you want to install a version not directly downloadable (for example
3.0.0-rc-2), you can download it from `Groovy's archive <http://apache.crihan.fr/dist/groovy/3.0.0-rc-2/sources/apache-groovy-src-3.0.0-rc-2.zip>`_.

Then, in a terminal, point to the folder where you have unzipped your
Groovy version and give it an alias (e.g., 3.0.0-rc-2SNAP): sdk
install groovy 3.0.0-rc-2SNAP /home/decide/Software/groovy-3.0.0-rc-2.

How to custom log messages with GeoClimate CLI?
-----------------------------------------------

GeoClimate CLI introduces a -l command to customize the messages printed by
GeoClimate.

.. code:: java

   // Shows info messages
   java -jar geoclimate.jar -f myconfigFile.json -l info

   // Shows debug messages
   java -jar geoclimate.jar -f myconfigFile.json -l debug

   // Shows trace messages
   java -jar geoclimate.jar -f myconfigFile.json -l trace

   // Turns off all messages
   java -jar geoclimate.jar -f myconfigFile.json -l off

Note that the log level info, debug, trace, off is applied for all
loggers loaded by GeoClimate, including third-party libraries.

The default log level is set to info.

How to compile the GeoClimate wiki documentation in PDF?
--------------------------------------------------------

Install the GitHub Wikito Converter available at
`GitHub Wikito Converter <https://github.com/yakivmospan/github-wikito-converter>`_.

::

   # Clone the wiki documentation
   git clone https://github.com/geoclimate/geoclimate.wiki.git

::

   # Convert the geoclimate.wiki folder
   gwtc -t "GeoClimate 1.0.0 documentation" --pdf-page-count --footer "GeoClimate - documentation 2020 - 2024 - Creative Commons Attribution-ShareAlike 4.0 International." -f pdf ./geoclimate.wiki/ -n GeoClimate_1.0.0_documentation

Combine the resulting PDF with the `PDF first page <https://github.com/orbisgis/geoclimate/blob/master/docs/resources/other/geoclimate_first_page.pdf>`_ (first updating versions and dates).

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

.. include:: _Footer.rst
