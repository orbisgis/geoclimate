# Execute the Geoclimate chain


## How to execute the Geoclimate chain

There are 2 ways to run the Geoclimate chain : from a Scripting Console or from the Command Line
Interface (CLI).


## Run Geoclimate from a scripting console

Geoclimate can, among other things, be executed using two tools presented below:

- [DBeaver](./execution_tools.md)
- [Groovy Console](./execution_tools.md)


## Run Geoclimate from the CLI (prefered)

The Geoclimate Command Line Interface (CLI) is a text-based user interface (UI) used to run
 Geoclimate chain in a Terminal from a provided configuration file.
 
 To run Geoclimate CLI set 
 
![](../resources/images/for_users/geoclimate_cli.png)

where

- -w  is the name of the workflow  : OSM or bdtopo_2.2
- -f  is the path to the workflow configuration file (json format)
- -h  shows help information
- -v  shows the Geoclimate version

Please follow this [link](https://github.com/orbisgis/geoclimate/tree/v1.0.0-RC1/processingchain/src/test/resources/org/orbisgis/orbisprocess/geoclimate/processingchain/config)
to find various configuration files.

The Geoclimate.jar is binary distribution that contains all necessary dependencies.

Up to data binary distribution is available at [here](https://jenkins.orbisgis.org/job/geoclimate-with-dependencies/lastSuccessfulBuild/artifact/geoclimate/target/Geoclimate.jar)

## Proxy configuration

Depending on networking environments, particularly corporate ones, you must have to deal with proxy configuration.

If you Geoclimate with a Groovy script, tune the proxy just like that

```java
System.getProperties().put("proxySet", true);
System.getProperties().put("proxyHost", "proxyUrl");
System.getProperties().put("proxyPort", "proxyPort");
```
If you use the Geoclimate CLI try this :

```java
java -Djava.net.useSystemProxies=true -Dhttp.proxyHost=10.10.10.10 -Dhttp.proxyPort=8080  -jar  Geoclimate.jar -f osm_geoclimate.json
```
if nothing works, please contact your system administrator ;-)



## Use cases

To see Geoclimate in action, you can consult the two following step-by-step documentations:

- With [OSM](./execution_examples/run_osm.md)
- With [BD Topo v2](./execution_examples/run_bd_topo_v2.md)

