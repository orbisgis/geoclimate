# Execution tools

You can execute the Geoclimate chain in many ways. Below are presented two simple (and free of charge) options:

- With [DBeaver](#With DBeaver)
- With the [Groovy console](#With the Groovy Console)



## With DBeaver

[DBeaver](https://dbeaver.io/) is a free and opensource multi-platform database tool to query, explore and manage data. Since the 6.2.2 version, DBeaver support the [H2GIS](http://h2gis.org/)-[H2 database](https://www.h2database.com/) engine. 

User is able to create a H2GIS database, query and display spatial objects from a friendly user interface (see https://twitter.com/H2GIS/status/1181566934548176897).

![](../resources/images/for_users/dbeaver_twitter.jpeg)



To use Geoclimate scripts in DBeaver, user must install the **Groovy Editor** and the **H2GIS drivers**, developed by the Geoclimate team.

In DBeaver:

1. Go to the Main menu `Help` -> `Install New Software...`
2. In the `Work with` field, paste the following URL http://devs.orbisgis.org/eclipse-repo (which is an extension P2 repository URL) and press `Enter`
3. Check the `OrbisGIS` item (in the sub-list, the `Groovy Editor` and the `h2gis_driver` should be selected too)
4. Click `Next` -> `Finish` and Restart DBeaver.

![](../resources/images/for_users/dbeaver_install_plugins.png)





------

### Remarks

#### Accept the license

You may be asked to accept the open license ([LGPL v3](https://www.gnu.org/licenses/lgpl-3.0.en.html)) of the `Groovy Editor` and the `h2gis_driver` . If so, check on `I accept the terms of the licence agreement` and click on `Finish`.

![dbeaver_accept_licence](../resources/images/for_users/dbeaver_accept_licence.png)



#### Security warning

You also may have a `Security Warning` since these two "plugins" are not officially developed by the DBeaver team. So you are asked to confirm the installation by clicking on `Install anyway`.

![dbeaver_install_anyway](../resources/images/for_users/dbeaver_install_anyway.png)

------



Once DBeaver has restarted, select the main menu `Groovy Editor`, click on `Open editor`, then you will have a Groovy Console.

![](../resources/images/for_users/dbeaver_groovy_console_text.png)



|                             Icon                             |              Action               |
| :----------------------------------------------------------: | :-------------------------------: |
| ![dbeaver_groovy_console_execute](../resources/images/for_users/dbeaver_groovy_console_execute.png) | Execute the selected instructions |
| ![dbeaver_groovy_console_execute_all](../resources/images/for_users/dbeaver_groovy_console_execute_all.png) |      Execute all the console      |
| ![dbeaver_groovy_console_erase](../resources/images/for_users/dbeaver_groovy_console_erase.png) |         Clear the console         |



Now you are ready to play with the Geoclimate chain by executing the dedicated scripts introduced below in the use cases.



## With the Groovy Console

Step by step tutorial with screenshots and command lines



## Use cases

Below are presented two step-by-step documentations to execute the OSM and BD Topo v2 workflows

- [OSM](./execution_examples/run_osm.md)
- [BD Topo v2](./execution_examples/run_bd_topo_v2.md)
