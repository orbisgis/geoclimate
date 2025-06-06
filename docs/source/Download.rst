Download
========
GeoClimate library uses a set of dependencies to run its algorithms. A GeoClimate package containing all these dependencies is compiled and published after every change on the source code on our Jenkins build.

For now we recommend using the last jar package currently being developped, you can get the last `snapshot version <https://nightly.link/orbisgis/geoclimate/workflows/CI_snapshot/master/geoclimate-1.0.1-SNAPSHOT>`_. It is consistent with the wiki documentation. If the snapshot version does not work as expected, please create `an issue <https://github.com/orbisgis/geoclimate/issues>`_ explaining the problem met. In this case you can also go to the `GeoClimate releases page <https://github.com/orbisgis/geoclimate/releases>`_ to download the last stable published jar package. Note that the documentation related to this version is different than the current wiki (you should instead refer to the documentation.pdf document that comes along with the GeoClimate version).

Both versions are ready to run in the command line interface.

If you use GeoClimate with GroovyConsole take care to set the good repository and the correct GeoClimate version.

.. code-block:: groovy

    @GrabResolver(name='orbisgis', root='https://oss.sonatype.org/content/repositories/snapshots')
    @Grab(group='org.orbisgis.geoclimate', module='geoclimate', version='0.0.2-SNAPSHOT')

**WARNING: adapt the version to the version of your choice**

.. include:: _Footer.rst
