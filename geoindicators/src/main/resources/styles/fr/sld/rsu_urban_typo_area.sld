<StyledLayerDescriptor xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc"
                       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.1.0"
                       units="mm"
                       xsi:schemaLocation="http://www.opengis.net/sld http://schemas.opengis.net/sld/1.1.0/StyledLayerDescriptor.xsd"
>
    <NamedLayer>
        <sld:Name>typo_usr_geom</sld:Name>
        <UserStyle>
            <sld:Name>typo_usr_geom</sld:Name>
            <sld:FeatureTypeStyle>
                <sld:Rule>
                    <sld:Name>Bâtiment d'activité</sld:Name>
                    <sld:Description>
                        <sld:Title>Bâtiment d'activité</sld:Title>
                    </sld:Description>
                    <MaxScaleDenominator>500000</MaxScaleDenominator>
                    <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>typo_maj</ogc:PropertyName>
                            <ogc:Literal>ba</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <sld:PolygonSymbolizer>
                        <sld:Fill>
                            <sld:SvgParameter name="fill">#8f8f8f</sld:SvgParameter>
                        </sld:Fill>
                        <sld:Stroke>
                            <sld:SvgParameter name="stroke">#000000</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-width">0.26</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-linejoin">bevel</sld:SvgParameter>
                        </sld:Stroke>
                    </sld:PolygonSymbolizer>
                </sld:Rule>
                <sld:Rule>
                    <sld:Name>Bâtiment de grande hauteur</sld:Name>
                    <sld:Description>
                        <sld:Title>Bâtiment de grande hauteur</sld:Title>
                    </sld:Description>
                    <MaxScaleDenominator>500000</MaxScaleDenominator>
                    <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>typo_maj</ogc:PropertyName>
                            <ogc:Literal>bgh</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <sld:PolygonSymbolizer>
                        <sld:Fill>
                            <sld:SvgParameter name="fill">#000d00</sld:SvgParameter>
                        </sld:Fill>
                        <sld:Stroke>
                            <sld:SvgParameter name="stroke">#000000</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-width">0.26</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-linejoin">bevel</sld:SvgParameter>
                        </sld:Stroke>
                    </sld:PolygonSymbolizer>
                </sld:Rule>
                <sld:Rule>
                    <sld:Name>Immeuble continu sur ilôt fermé</sld:Name>
                    <sld:Description>
                        <sld:Title>Immeuble continu sur ilôt fermé</sld:Title>
                    </sld:Description>
                    <MaxScaleDenominator>500000</MaxScaleDenominator>
                    <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>typo_maj</ogc:PropertyName>
                            <ogc:Literal>icif</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <sld:PolygonSymbolizer>
                        <sld:Fill>
                            <sld:SvgParameter name="fill">#d52623</sld:SvgParameter>
                        </sld:Fill>
                        <sld:Stroke>
                            <sld:SvgParameter name="stroke">#000000</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-width">0.26</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-linejoin">bevel</sld:SvgParameter>
                        </sld:Stroke>
                    </sld:PolygonSymbolizer>
                </sld:Rule>
                <sld:Rule>
                    <sld:Name>Immeuble continu sur ilôt ouvert</sld:Name>
                    <sld:Description>
                        <sld:Title>Immeuble continu sur ilôt ouvert</sld:Title>
                    </sld:Description>
                    <MaxScaleDenominator>500000</MaxScaleDenominator>
                    <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>typo_maj</ogc:PropertyName>
                            <ogc:Literal>icio</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <sld:PolygonSymbolizer>
                        <sld:Fill>
                            <sld:SvgParameter name="fill">#f07923</sld:SvgParameter>
                        </sld:Fill>
                        <sld:Stroke>
                            <sld:SvgParameter name="stroke">#000000</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-width">0.26</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-linejoin">bevel</sld:SvgParameter>
                        </sld:Stroke>
                    </sld:PolygonSymbolizer>
                </sld:Rule>
                <sld:Rule>
                    <sld:Name>Immeuble discontinu</sld:Name>
                    <sld:Description>
                        <sld:Title>Immeuble discontinu</sld:Title>
                    </sld:Description>
                    <MaxScaleDenominator>500000</MaxScaleDenominator>
                    <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>typo_maj</ogc:PropertyName>
                            <ogc:Literal>id</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <sld:PolygonSymbolizer>
                        <sld:Fill>
                            <sld:SvgParameter name="fill">#eccb27</sld:SvgParameter>
                        </sld:Fill>
                        <sld:Stroke>
                            <sld:SvgParameter name="stroke">#000000</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-width">0.26</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-linejoin">bevel</sld:SvgParameter>
                        </sld:Stroke>
                    </sld:PolygonSymbolizer>
                </sld:Rule>
                <sld:Rule>
                    <sld:Name>Local</sld:Name>
                    <sld:Description>
                        <sld:Title>Local</sld:Title>
                    </sld:Description>
                    <MaxScaleDenominator>500000</MaxScaleDenominator>
                    <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>typo_maj</ogc:PropertyName>
                            <ogc:Literal>local</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <sld:PolygonSymbolizer>
                        <sld:Fill>
                            <sld:SvgParameter name="fill">#d728ac</sld:SvgParameter>
                        </sld:Fill>
                        <sld:Stroke>
                            <sld:SvgParameter name="stroke">#000000</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-width">0.26</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-linejoin">bevel</sld:SvgParameter>
                        </sld:Stroke>
                    </sld:PolygonSymbolizer>
                </sld:Rule>
                <sld:Rule>
                    <sld:Name>Pavillon continu sur ilôt fermé</sld:Name>
                    <sld:Description>
                        <sld:Title>Pavillon continu sur ilôt fermé</sld:Title>
                    </sld:Description>
                    <MaxScaleDenominator>500000</MaxScaleDenominator>
                    <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>typo_maj</ogc:PropertyName>
                            <ogc:Literal>pcif</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <sld:PolygonSymbolizer>
                        <sld:Fill>
                            <sld:SvgParameter name="fill">#2b6724</sld:SvgParameter>
                        </sld:Fill>
                        <sld:Stroke>
                            <sld:SvgParameter name="stroke">#000000</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-width">0.26</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-linejoin">bevel</sld:SvgParameter>
                        </sld:Stroke>
                    </sld:PolygonSymbolizer>
                </sld:Rule>
                <sld:Rule>
                    <sld:Name>Pavillon continu sur ilôt ouvert</sld:Name>
                    <sld:Description>
                        <sld:Title>Pavillon continu sur ilôt ouvert</sld:Title>
                    </sld:Description>
                    <MaxScaleDenominator>500000</MaxScaleDenominator>
                    <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>typo_maj</ogc:PropertyName>
                            <ogc:Literal>pcio</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <sld:PolygonSymbolizer>
                        <sld:Fill>
                            <sld:SvgParameter name="fill">#36884a</sld:SvgParameter>
                        </sld:Fill>
                        <sld:Stroke>
                            <sld:SvgParameter name="stroke">#000000</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-width">0.26</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-linejoin">bevel</sld:SvgParameter>
                        </sld:Stroke>
                    </sld:PolygonSymbolizer>
                </sld:Rule>
                <sld:Rule>
                    <sld:Name>Pavillon discontinu</sld:Name>
                    <sld:Description>
                        <sld:Title>Pavillon discontinu</sld:Title>
                    </sld:Description>
                    <MaxScaleDenominator>500000</MaxScaleDenominator>
                    <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>typo_maj</ogc:PropertyName>
                            <ogc:Literal>pd</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <sld:PolygonSymbolizer>
                        <sld:Fill>
                            <sld:SvgParameter name="fill">#22be2f</sld:SvgParameter>
                        </sld:Fill>
                        <sld:Stroke>
                            <sld:SvgParameter name="stroke">#000000</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-width">0.26</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-linejoin">bevel</sld:SvgParameter>
                        </sld:Stroke>
                    </sld:PolygonSymbolizer>
                </sld:Rule>
                <sld:Rule>
                    <sld:Name>Pavillon semi-continu</sld:Name>
                    <sld:Description>
                        <sld:Title>Pavillon semi-continu</sld:Title>
                    </sld:Description>
                    <MaxScaleDenominator>500000</MaxScaleDenominator>
                    <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
                        <ogc:PropertyIsEqualTo>
                            <ogc:PropertyName>typo_maj</ogc:PropertyName>
                            <ogc:Literal>psc</ogc:Literal>
                        </ogc:PropertyIsEqualTo>
                    </ogc:Filter>
                    <sld:PolygonSymbolizer>
                        <sld:Fill>
                            <sld:SvgParameter name="fill">#05ff58</sld:SvgParameter>
                        </sld:Fill>
                        <sld:Stroke>
                            <sld:SvgParameter name="stroke">#000000</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-width">0.26</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-linejoin">bevel</sld:SvgParameter>
                        </sld:Stroke>
                    </sld:PolygonSymbolizer>
                </sld:Rule>
                <sld:Rule>
                    <sld:Name>Inconnu</sld:Name>
                    <sld:Description>
                        <sld:Title>Inconnu</sld:Title>
                    </sld:Description>
                    <MaxScaleDenominator>500000</MaxScaleDenominator>
                    <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
                        <ogc:PropertyIsNull>
                            <ogc:PropertyName>typo_maj</ogc:PropertyName>
                        </ogc:PropertyIsNull>
                    </ogc:Filter>
                    <sld:PolygonSymbolizer>
                        <sld:Fill>
                            <sld:SvgParameter name="fill">#ffffff</sld:SvgParameter>
                        </sld:Fill>
                        <sld:Stroke>
                            <sld:SvgParameter name="stroke">#000000</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-width">0.26</sld:SvgParameter>
                            <sld:SvgParameter name="stroke-linejoin">bevel</sld:SvgParameter>
                        </sld:Stroke>
                    </sld:PolygonSymbolizer>
                </sld:Rule>
            </sld:FeatureTypeStyle>
        </UserStyle>
    </NamedLayer>
</StyledLayerDescriptor>