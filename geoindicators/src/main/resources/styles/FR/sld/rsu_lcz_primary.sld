<?xml version="1.0" encoding="UTF-8"?><sld:StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:sld="http://www.opengis.net/sld" xmlns:gml="http://www.opengis.net/gml" xmlns:ogc="http://www.opengis.net/ogc" version="1.0.0">
  <sld:NamedLayer>
    <sld:Name>rsu_lcz</sld:Name>
    <sld:UserStyle>
      <sld:Name>rsu_lcz</sld:Name>
      <sld:FeatureTypeStyle>
        <sld:Name>name</sld:Name>
        <sld:Rule>
          <sld:Name>ZCL 1: Ensemble de tours compact</sld:Name>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:PropertyName>LCZ_PRIMARY</ogc:PropertyName>
              <ogc:Literal>1</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#8B0101</sld:CssParameter>
            </sld:Fill>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>ZCL 2: Ensemble d'immeubles compact</sld:Name>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:PropertyName>LCZ_PRIMARY</ogc:PropertyName>
              <ogc:Literal>2</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#cc0200</sld:CssParameter>
            </sld:Fill>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>ZCL 3: Ensemble de maisons compact</sld:Name>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:PropertyName>LCZ_PRIMARY</ogc:PropertyName>
              <ogc:Literal>3</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#fc0001</sld:CssParameter>
            </sld:Fill>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>ZCL 4: Ensemble de tours espacées</sld:Name>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:PropertyName>LCZ_PRIMARY</ogc:PropertyName>
              <ogc:Literal>4</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#be4c03</sld:CssParameter>
            </sld:Fill>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>ZCL 5: Ensemble d'immeubles espacés</sld:Name>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:PropertyName>LCZ_PRIMARY</ogc:PropertyName>
              <ogc:Literal>5</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#ff6602</sld:CssParameter>
            </sld:Fill>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>ZCL 6: Ensemble de maisons espacées</sld:Name>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:PropertyName>LCZ_PRIMARY</ogc:PropertyName>
              <ogc:Literal>6</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#ff9856</sld:CssParameter>
            </sld:Fill>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>ZCL 7: Constructions légères</sld:Name>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:PropertyName>LCZ_PRIMARY</ogc:PropertyName>
              <ogc:Literal>7</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#fbed08</sld:CssParameter>
            </sld:Fill>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>ZCL 8: Grandes emprises tertiaires bâti bas</sld:Name>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:PropertyName>LCZ_PRIMARY</ogc:PropertyName>
              <ogc:Literal>8</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#bcbcba</sld:CssParameter>
            </sld:Fill>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>ZCL 9: Maisons éparses, constructions isolées</sld:Name>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:PropertyName>LCZ_PRIMARY</ogc:PropertyName>
              <ogc:Literal>9</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#ffcca7</sld:CssParameter>
            </sld:Fill>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>ZCL 10: Industrie lourde</sld:Name>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:PropertyName>LCZ_PRIMARY</ogc:PropertyName>
              <ogc:Literal>10</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#57555a</sld:CssParameter>
            </sld:Fill>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>LCZ A: Dense trees</sld:Name>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:PropertyName>LCZ_PRIMARY</ogc:PropertyName>
              <ogc:Literal>101</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#006700</sld:CssParameter>
            </sld:Fill>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>ZCL B: Arbres épars</sld:Name>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:PropertyName>LCZ_PRIMARY</ogc:PropertyName>
              <ogc:Literal>102</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#05aa05</sld:CssParameter>
            </sld:Fill>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>ZCL C: Buissons, broussaille</sld:Name>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:PropertyName>LCZ_PRIMARY</ogc:PropertyName>
              <ogc:Literal>103</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#648423</sld:CssParameter>
            </sld:Fill>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>ZCL D: Pelouses, praires, cultures</sld:Name>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:PropertyName>LCZ_PRIMARY</ogc:PropertyName>
              <ogc:Literal>104</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#bbdb7a</sld:CssParameter>
            </sld:Fill>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>ZCL E: Roche nue, pavés, macadam</sld:Name>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:PropertyName>LCZ_PRIMARY</ogc:PropertyName>
              <ogc:Literal>105</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#010101</sld:CssParameter>
            </sld:Fill>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>ZCL F: Terrain nu ou sable</sld:Name>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:PropertyName>LCZ_PRIMARY</ogc:PropertyName>
              <ogc:Literal>106</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
          <sld:PolygonSymbolizer>
             <sld:Fill>
              <sld:CssParameter name="fill">#fdf6ae</sld:CssParameter>
            </sld:Fill>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>ZCL G: Eau</sld:Name>
          <ogc:Filter>
            <ogc:PropertyIsEqualTo>
              <ogc:PropertyName>LCZ_PRIMARY</ogc:PropertyName>
              <ogc:Literal>107</ogc:Literal>
            </ogc:PropertyIsEqualTo>
          </ogc:Filter>
          <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#6d67fd</sld:CssParameter>
            </sld:Fill>
          </sld:PolygonSymbolizer>
        </sld:Rule>
        <sld:Rule>
          <sld:Name>Inconnu</sld:Name>
          <ogc:Filter>
            <ogc:PropertyIsNull>
              <ogc:PropertyName>LCZ_PRIMARY</ogc:PropertyName>
            </ogc:PropertyIsNull>
          </ogc:Filter>
          <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
          <sld:PolygonSymbolizer>
            <sld:Fill>
              <sld:CssParameter name="fill">#ffffff</sld:CssParameter>
            </sld:Fill>
          </sld:PolygonSymbolizer>
        </sld:Rule>
      </sld:FeatureTypeStyle>
    </sld:UserStyle>
  </sld:NamedLayer>
</sld:StyledLayerDescriptor>