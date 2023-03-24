<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor xmlns="http://www.opengis.net/sld"
                       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.1.0"
                       xmlns:ogc="http://www.opengis.net/ogc"
                       xsi:schemaLocation="http://www.opengis.net/sld http://schemas.opengis.net/sld/1.1.0/StyledLayerDescriptor.xsd"
>
    <NamedLayer>
        <sld:Name>Average building compactness</sld:Name>
        <UserStyle>
            <sld:Name>Average building compactness</sld:Name>
            <sld:FeatureTypeStyle>
                <sld:Rule>
                    <sld:Name>0.0 - 0.0</sld:Name>
                    <sld:Description>
                        <sld:Title>0.0 - 0.0</sld:Title>
                    </sld:Description>
                    <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>NET_COMPACTNESS</ogc:PropertyName>
                            <ogc:Literal>0</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
                    <sld:PolygonSymbolizer>
                        <sld:Fill>
                            <sld:SvgParameter name="fill">#feebe0</sld:SvgParameter>
                        </sld:Fill>
                        <sld:Stroke>
                            <sld:SvgParameter name="stroke">#232323</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-width">1</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-linejoin">bevel</sld:SvgParameter>
                        </sld:Stroke>
                    </sld:PolygonSymbolizer>
                </sld:Rule>
                <sld:Rule>
                    <sld:Name>0.0 - 4.6</sld:Name>
                    <sld:Description>
                        <sld:Title>0.0 - 4.6</sld:Title>
                    </sld:Description>
                    <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
                        <ogc:And>
                            <ogc:PropertyIsGreaterThan>
                                <ogc:PropertyName>NET_COMPACTNESS</ogc:PropertyName>
                                <ogc:Literal>0</ogc:Literal>
                            </ogc:PropertyIsGreaterThan>
                            <ogc:PropertyIsLessThanOrEqualTo>
                                <ogc:PropertyName>NET_COMPACTNESS</ogc:PropertyName>
                                <ogc:Literal>4.59999999999999964</ogc:Literal>
                            </ogc:PropertyIsLessThanOrEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
                    <sld:PolygonSymbolizer>
                        <sld:Fill>
                            <sld:SvgParameter name="fill">#ffaab6</sld:SvgParameter>
                        </sld:Fill>
                        <sld:Stroke>
                            <sld:SvgParameter name="stroke">#232323</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-width">1</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-linejoin">bevel</sld:SvgParameter>
                        </sld:Stroke>
                    </sld:PolygonSymbolizer>
                </sld:Rule>
                <sld:Rule>
                    <sld:Name>4.6 - 5.0</sld:Name>
                    <sld:Description>
                        <sld:Title>4.6 - 5.0</sld:Title>
                    </sld:Description>
                    <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
                        <ogc:And>
                            <ogc:PropertyIsGreaterThan>
                                <ogc:PropertyName>NET_COMPACTNESS</ogc:PropertyName>
                                <ogc:Literal>4.59999999999999964</ogc:Literal>
                            </ogc:PropertyIsGreaterThan>
                            <ogc:PropertyIsLessThanOrEqualTo>
                                <ogc:PropertyName>NET_COMPACTNESS</ogc:PropertyName>
                                <ogc:Literal>5</ogc:Literal>
                            </ogc:PropertyIsLessThanOrEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
                    <sld:PolygonSymbolizer>
                        <sld:Fill>
                            <sld:SvgParameter name="fill">#ff29a0</sld:SvgParameter>
                        </sld:Fill>
                        <sld:Stroke>
                            <sld:SvgParameter name="stroke">#232323</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-width">1</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-linejoin">bevel</sld:SvgParameter>
                        </sld:Stroke>
                    </sld:PolygonSymbolizer>
                </sld:Rule>
                <sld:Rule>
                    <sld:Name>5.0 - 5.5</sld:Name>
                    <sld:Description>
                        <sld:Title>5.0 - 5.5</sld:Title>
                    </sld:Description>
                    <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
                        <ogc:And>
                            <ogc:PropertyIsGreaterThan>
                                <ogc:PropertyName>NET_COMPACTNESS</ogc:PropertyName>
                                <ogc:Literal>5</ogc:Literal>
                            </ogc:PropertyIsGreaterThan>
                            <ogc:PropertyIsLessThanOrEqualTo>
                                <ogc:PropertyName>NET_COMPACTNESS</ogc:PropertyName>
                                <ogc:Literal>5.5</ogc:Literal>
                            </ogc:PropertyIsLessThanOrEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
                    <sld:PolygonSymbolizer>
                        <sld:Fill>
                            <sld:SvgParameter name="fill">#d1008b</sld:SvgParameter>
                        </sld:Fill>
                        <sld:Stroke>
                            <sld:SvgParameter name="stroke">#232323</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-width">1</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-linejoin">bevel</sld:SvgParameter>
                        </sld:Stroke>
                    </sld:PolygonSymbolizer>
                </sld:Rule>
                <sld:Rule>
                    <sld:Name>5.5 - 103</sld:Name>
                    <sld:Description>
                        <sld:Title>5.5 - 103</sld:Title>
                    </sld:Description>
                    <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
                        <ogc:And>
                            <ogc:PropertyIsGreaterThan>
                                <ogc:PropertyName>NET_COMPACTNESS</ogc:PropertyName>
                                <ogc:Literal>5.5</ogc:Literal>
                            </ogc:PropertyIsGreaterThan>
                            <ogc:PropertyIsLessThanOrEqualTo>
                                <ogc:PropertyName>NET_COMPACTNESS</ogc:PropertyName>
                                <ogc:Literal>103</ogc:Literal>
                            </ogc:PropertyIsLessThanOrEqualTo>
                        </ogc:And>
                    </ogc:Filter>
                    <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
                    <sld:PolygonSymbolizer>
                        <sld:Fill>
                            <sld:SvgParameter name="fill">#86007a</sld:SvgParameter>
                        </sld:Fill>
                        <sld:Stroke>
                            <sld:SvgParameter name="stroke">#232323</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-width">1</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-linejoin">bevel</sld:SvgParameter>
                        </sld:Stroke>
                    </sld:PolygonSymbolizer>
                </sld:Rule>
            </sld:FeatureTypeStyle>
        </UserStyle>
    </NamedLayer>
</StyledLayerDescriptor>
