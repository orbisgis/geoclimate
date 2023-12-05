<?xml version="1.0" encoding="UTF-8"?>
<sld:StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:sld="http://www.opengis.net/sld"
                           xmlns:ogc="http://www.opengis.net/ogc"
                           version="1.0.0">
    <sld:NamedLayer>
        <sld:UserStyle>
            <sld:FeatureTypeStyle>
                <sld:Rule>
                    <sld:Name>LCZ 1: Compact high-rise</sld:Name>
                    <sld:Description>
                        <sld:Title>LCZ 1: Compact high-rise</sld:Title>
                    </sld:Description>
                    <ogc:Filter>
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>LCZ_PRIMARY</ogc:PropertyName>
                            <ogc:Literal>1</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <sld:MaxScaleDenominator>500000</sld:MaxScaleDenominator>
                    <sld:PolygonSymbolizer>
                        <sld:Fill>
                            <sld:CssParameter name="fill">#8b0101</sld:CssParameter>
                        </sld:Fill>
                    </sld:PolygonSymbolizer>
                </sld:Rule>
                <sld:Rule>
                    <sld:Name>LCZ 2: Compact mid-rise</sld:Name>
                    <sld:Description>
                        <sld:Title>LCZ 2: Compact mid-rise</sld:Title>
                    </sld:Description>
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
                    <sld:Name>LCZ 3: Compact low-rise</sld:Name>
                    <sld:Description>
                        <sld:Title>LCZ 3: Compact low-rise</sld:Title>
                    </sld:Description>
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
                    <sld:Name>LCZ 4: Open high-rise</sld:Name>
                    <sld:Description>
                        <sld:Title>LCZ 4: Open high-rise</sld:Title>
                    </sld:Description>
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
                    <sld:Name>LCZ 5: Open mid-rise</sld:Name>
                    <sld:Description>
                        <sld:Title>LCZ 5: Open mid-rise</sld:Title>
                    </sld:Description>
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
                    <sld:Name>LCZ 6: Open low-rise</sld:Name>
                    <sld:Description>
                        <sld:Title>LCZ 6: Open low-rise</sld:Title>
                    </sld:Description>
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
                    <sld:Name>LCZ 7: Lightweight low-rise</sld:Name>
                    <sld:Description>
                        <sld:Title>LCZ 7: Lightweight low-rise</sld:Title>
                    </sld:Description>
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
                    <sld:Name>LCZ 8: Large low-rise</sld:Name>
                    <sld:Description>
                        <sld:Title>LCZ 8: Large low-rise</sld:Title>
                    </sld:Description>
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
                    <sld:Name>LCZ 9: Sparsely built</sld:Name>
                    <sld:Description>
                        <sld:Title>LCZ 9: Sparsely built</sld:Title>
                    </sld:Description>
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
                    <sld:Name>LCZ 10: Heavy industry</sld:Name>
                    <sld:Description>
                        <sld:Title>LCZ 10: Heavy industry</sld:Title>
                    </sld:Description>
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
                    <sld:Description>
                        <sld:Title>LCZ A: Dense trees</sld:Title>
                    </sld:Description>
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
                    <sld:Name>LCZ B: Scattered trees</sld:Name>
                    <sld:Description>
                        <sld:Title>LCZ B: Scattered trees</sld:Title>
                    </sld:Description>
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
                    <sld:Name>LCZ C: Bush,scrub</sld:Name>
                    <sld:Description>
                        <sld:Title>LCZ C: Bush,scrub</sld:Title>
                    </sld:Description>
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
                    <sld:Name>LCZ D: Low plants</sld:Name>
                    <sld:Description>
                        <sld:Title>LCZ D: Low plants</sld:Title>
                    </sld:Description>
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
                    <sld:Name>LCZ E: Bare rock or paved</sld:Name>
                    <sld:Description>
                        <sld:Title>LCZ E: Bare rock or paved</sld:Title>
                    </sld:Description>
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
                    <sld:Name>LCZ F: Bare soil or sand</sld:Name>
                    <sld:Description>
                        <sld:Title>LCZ F: Bare soil or sand</sld:Title>
                    </sld:Description>
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
                    <sld:Name>LCZ G: Water</sld:Name>
                    <sld:Description>
                        <sld:Title>LCZ G: Water</sld:Title>
                    </sld:Description>
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
                    <sld:Name>Undefined</sld:Name>
                    <sld:Description>
                        <sld:Title>Undefined</sld:Title>
                    </sld:Description>
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
