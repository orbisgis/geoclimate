Display Output with QGIS
========================

You can display the generated database with **QGIS**. To do this:

- Open QGIS.
- Open the folder that contains the outputs.
- Drag and drop the content of the output folder into QGIS.

.. figure:: /_static/other/show_data_tutorial/import.gif

Tweak the Displayed Layers
--------------------------

To manage which layers are displayed, you can check or uncheck the boxes next to each layer on the left panel to show or hide them.
You can also change the order of the layers by rearranging them in the **Layers** panel.

.. figure:: /_static/other/show_data_tutorial/layers_manipulation.gif

Apply Style to the RSU_LCZ Layer
--------------------------------

To properly display the `RSU_LCZ` layer, we recommend applying the following style file `rsu_lcz_primary.sld <_static/other/rsu_lcz_primary.sld>`__

To apply it:

1. Download the `.sld` style file using the link above.
2. In QGIS, right-click on the `RSU_LCZ` layer and select **Properties**.
3. At the bottom of the popup, click the **Style** button, then choose **Load Style**.
4. In the "Load Style" dialog, select **From a file**.
5. Click the `...` button, browse to your `.sld` file, and select it.
6. Click the **Load Style** button at the bottom to apply the style.

.. figure:: /_static/other/show_data_tutorial/rsu_lcz_style.gif

-----------------------------------------------------------

.. include:: _Footer.rst
