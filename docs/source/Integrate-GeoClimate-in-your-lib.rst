Integrate Geoclimate in your lib
=================================

For advanced users or developers, GeoClimate and its dependencies can be
grabbed from https://oss.sonatype.org using Maven.

Set in your pom the following information

.. code:: xml

    <dependencies>
    <dependency>
      <groupId>org.orbisgis.geoclimate</groupId>
      <artifactId>geoclimate</artifactId>
      <version>1.0.0</version>
    </dependency>
    </dependencies>

    <repositories>
             <repository>
                <id>orbisgis-release</id>
                <url>https://oss.sonatype.org/content/repositories/releases/</url>
                <snapshots>
                    <enabled>false</enabled>
                </snapshots>
                <releases>
                    <enabled>true</enabled>
                </releases>
            </repository>
            <repository>
                <id>orbisgis-snapshot</id>
                <url>https://oss.sonatype.org/content/repositories/snapshots/</url>
                <snapshots>
                    <updatePolicy>always</updatePolicy>
                    <enabled>true</enabled>
                </snapshots>
                <releases>
                    <enabled>false</enabled>
                </releases>
            </repository>
    </repositories>

-------------------

.. include:: _Footer.rst
