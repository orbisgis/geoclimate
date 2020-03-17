-- SQL script used to create the building and reference spatial unit (RSU) Tables that will be used to test the validity of the indicators.
-- Note that all fields respect the generic names defined within the PAENDORA and URCLIM projects.
-- Each RSU contains one or several buildings having different vertical and horizontal sizes.

-- NOTE: "rsu_building_density" and "rsu_free_external_facade_density" (stored in the "rsu_test" table are calculated considering only the 7 first buildings)
-- NOTE: "rsu_mean_building_height" (stored in the "rsu_build_corr" table are calculated considering only the 7 first buildings)


DROP TABLE IF EXISTS building_test;
DROP TABLE IF EXISTS block_test;
DROP TABLE IF EXISTS block_build_corr;
DROP TABLE IF EXISTS rsu_test;
DROP TABLE IF EXISTS rsu_build_corr;
DROP TABLE IF EXISTS road_test;
DROP TABLE IF EXISTS veget_test;
DROP TABLE IF EXISTS hydro_test;
DROP TABLE IF EXISTS rsu_test_for_lcz;

CREATE TABLE building_test (id_build int, id_block int, id_rsu int, zindex int, the_geom geometry, height_wall float, height_roof float, area float, perimeter float, nb_lev int, total_facade_length float, number_building_neighbor int, contiguity float);
CREATE TABLE block_test (id_block int, the_geom geometry);
CREATE TABLE block_build_corr (id_block int, id_build int);
CREATE TABLE rsu_test (id_rsu int, the_geom geometry, rsu_area float, rsu_building_density float, rsu_free_external_facade_density float);
CREATE TABLE rsu_build_corr (id_rsu int, id_build int, rsu_mean_building_height float);
CREATE TABLE road_test (id_road int, the_geom geometry, width float, zindex int, crossing varchar(30));
CREATE TABLE veget_test (id_veget int, the_geom geometry, height_class varchar);
CREATE TABLE hydro_test (id_hydro int, the_geom geometry);
CREATE TABLE rsu_test_for_lcz(id_rsu int, sky_view_factor float, aspect_ratio float, building_surface_fraction float,
impervious_surface_fraction float, pervious_surface_fraction float, height_of_roughness_elements float,
terrain_roughness_class float);

INSERT INTO building_test VALUES (1, 1, 1,'POLYGON((4 4, 10 4, 10 30, 4 30, 4 4))'::GEOMETRY, 8, 8, 156, 64, 2, 64, 0, 0),
 (2, 2, 1, 'POLYGON((12 4, 20 4, 20 9, 12 9, 12 4))'::GEOMETRY, 10, 13, 40, 26, 3, 26, 0, 0),
 (3, 3, 1,'POLYGON((25 4, 45 4, 45 9, 25 9, 25 4))'::GEOMETRY, 8, 14, 100, 50, 4, 50, 0, 0),
 (4, 4, 1,'POLYGON((25 25, 40 25, 40 37, 25 37, 25 25))'::GEOMETRY, 5, 8, 180, 54, 2, 54, 1, 0.18518518518518517),
 (5, 4, 1,'POLYGON((12 25, 25 25, 25 35, 12 35, 12 25))'::GEOMETRY, 12, 12, 130, 46, 4, 46, 1, 0.09057971014492754),
 (6, 5, 2,'POLYGON((52 2, 54 2, 54 10, 52 10, 52 2))'::GEOMETRY, 15, 18, 16, 20, 5, 20, 0, 0),
 (7, 6, 3,'POLYGON((0 -5, 10 -5, 10 0, 0 0, 0 -5), (1 -4, 2 -4, 2 -1, 1 -1, 1 -4))'::GEOMETRY, 3, 3, 47, 30, 1, 38, 0, 0),
 (8, 4, 1,'POLYGON((30 25, 40 15, 45 20, 40 25, 30 25))'::GEOMETRY, 4, 4, 75, 38.284, 1, 38.284, 1, null),
 (9, null, 4,'POLYGON((1020 1000, 1040 1000, 1040 1100, 1020 1100, 1020 1000))'::GEOMETRY, 20, 20, null, null, null, null, null, null),
 (10, null, 4,st_translate('POLYGON((1020 1000, 1040 1000, 1040 1100, 1020 1100, 1020 1000))'::GEOMETRY, 40, 0), 20, 20, null, null, null, null, null, null),
 (11, null, 5,st_translate('POLYGON((1020 1000, 1040 1000, 1040 1100, 1020 1100, 1020 1000))'::GEOMETRY, 100, 0), 20, 20, null, null, null, null, null, null),
 (12, null, 5,st_translate('POLYGON((1020 1000, 1040 1000, 1040 1100, 1020 1100, 1020 1000))'::GEOMETRY, 140, 0), 20, 20, null, null, null, null, null, null),
 (13, null, 6,st_translate('POLYGON((1020 1000, 1040 1000, 1040 1100, 1020 1100, 1020 1000))'::GEOMETRY, 200, 0), 20, 20, null, null, null, null, null, null),
 (14, null, 6,st_translate('POLYGON((1020 1000, 1040 1000, 1040 1100, 1020 1100, 1020 1000))'::GEOMETRY, 240, 0), 20, 20, null, null, null, null, null, null),
 (15, null, 7,st_translate('POLYGON((1020 1000, 1040 1000, 1040 1100, 1020 1100, 1020 1000))'::GEOMETRY, 0, 100), 20, 20, null, null, null, null, null, null),
 (16, null, 7,st_translate('POLYGON((1020 1000, 1040 1000, 1040 1100, 1020 1100, 1020 1000))'::GEOMETRY, 40, 100), 20, 20, null, null, null, null, null, null),
 (17, null, 8,st_translate('POLYGON((1020 1000, 1040 1000, 1040 1100, 1020 1100, 1020 1000))'::GEOMETRY, 100, 100), 20, 20, null, null, null, null, null, null),
 (18, null, 8,st_translate('POLYGON((1020 1000, 1040 1000, 1040 1100, 1020 1100, 1020 1000))'::GEOMETRY, 140, 100), 20, 20, null, null, null, null, null, null),
 (19, null, 9,st_translate('POLYGON((1020 1000, 1040 1000, 1040 1100, 1020 1100, 1020 1000))'::GEOMETRY, 200, 100), 20, 20, null, null, null, null, null, null),
 (20, null, 9,st_translate('POLYGON((1020 1000, 1040 1000, 1040 1100, 1020 1100, 1020 1000))'::GEOMETRY, 240, 100), 20, 20, null, null, null, null, null, null),
 (21, null, 10,st_translate('POLYGON((1020 1000, 1040 1000, 1040 1100, 1020 1100, 1020 1000))'::GEOMETRY, 0, 200), 20, 20, null, null, null, null, null, null),
 (22, null, 10,st_translate('POLYGON((1020 1000, 1040 1000, 1040 1100, 1020 1100, 1020 1000))'::GEOMETRY, 40, 200), 20, 20, null, null, null, null, null, null),
 (23, null, 11,st_translate('POLYGON((1020 1000, 1040 1000, 1040 1100, 1020 1100, 1020 1000))'::GEOMETRY, 100, 200), 20, 20, null, null, null, null, null, null),
 (24, null, 11,st_translate('POLYGON((1020 1000, 1040 1000, 1040 1100, 1020 1100, 1020 1000))'::GEOMETRY, 140, 200), 20, 20, null, null, null, null, null, null),
 (25, null, 12,st_translate('POLYGON((1020 1000, 1040 1000, 1040 1100, 1020 1100, 1020 1000))'::GEOMETRY, 200, 200), 20, 20, null, null, null, null, null, null),
 (26, null, 12,st_translate('POLYGON((1020 1000, 1040 1000, 1040 1100, 1020 1100, 1020 1000))'::GEOMETRY, 240, 200), 20, 20, null, null, null, null, null, null),
 (27, null, 13,st_translate('POLYGON((1210 1190, 1230 1210, 1230 1230, 1210 1230, 1210 1190))'::GEOMETRY, 200, 200), 5, 15, 600, 108.2842712474619, 4, 108.2842712474619, 0, null),
 (28, null, 13,st_translate('POLYGON((1310 1190, 1330 1210, 1330 1230, 1280 1230, 1310 1190))'::GEOMETRY, 200, 200), 18, 32, 1200, 148.2842712474619, 10, 148.2842712474619, 0, null),
 (29, 7, 3,'POLYGON((20 -2, 21 -2, 21 1, 20 1, 20 -2))'::GEOMETRY, 3, 5, 3, 8, 1, 8, 0, 0),
 (30, null, null, 'POLYGON((4 36, 6 36, 6 40, 4 40, 4 39, 5 39, 5 38, 4 38, 4 36))'::GEOMETRY, 3, 5, 7, 14, null, null, null, null),
 (31, 8, null, 'POLYGON((5 -145, 20 -145, 20 -105, 5 -105, 5 -145),(10 -130, 15 -130, 15 -110, 10 -110, 10 -130))'::GEOMETRY, 10, 10, null, null, null, null, null, null),
 (32, 8, null, 'POLYGON((20 -110, 45 -110, 45 -105, 20 -105, 20 -110))'::GEOMETRY, 15, 17, null, null, null, null, null, null),
 (33, 8, null, 'POLYGON((35 -145, 45 -145, 45 -110, 35 -110, 35 -145))'::GEOMETRY, 12, 12, null, null, null, null, null, null),
 (34, 8, null, 'POLYGON((20 -145, 35 -145, 35 -140, 15 -130, 20 -145))'::GEOMETRY, 12, 15, null, null, null, null, null, null),
 (35, null, null, 'POLYGON((-31 10, -29 10, -29 20, -31 20, -31 10))'::GEOMETRY, 20, 20, null, null, null, null, null, null),
 (36, null, null, st_translate('POLYGON((1200 1200, 1300 1200, 1300 1300, 1200 1300, 1200 1200))'::GEOMETRY, 300, 300), 20, 20, null, null, null, null, null, null);

INSERT INTO block_test VALUES (1, 'POLYGON((4 4, 10 4, 10 30, 4 30, 4 4))'::GEOMETRY),
 (2, 'POLYGON((12 4, 20 4, 20 9, 12 9, 12 4))'::GEOMETRY),
 (3, 'POLYGON((25 4, 45 4, 45 9, 25 9, 25 4))'::GEOMETRY),
 (4, 'POLYGON((25 25, 30 25, 40 15, 45 20, 40 25, 40 37, 25 37, 25 35, 12 35, 12 25, 25 25))'::GEOMETRY),
 (5, 'POLYGON((52 2, 54 2, 54 10, 52 10, 52 2))'::GEOMETRY),
 (6, 'POLYGON((0 -5, 10 -5, 10 0, 0 0, 0 -5), (1 -4, 2 -4, 2 -1, 1 -1, 1 -4))'::GEOMETRY),
 (7, 'POLYGON((20 -2, 21 -2, 21 1, 20 1, 20 -2))'::GEOMETRY),
 (8, 'POLYGON((5 -145, 45 -145, 45 -105, 5 -105, 5 -145), (10 -130, 15 -130, 15 -110, 10 -110, 10 -130), (20 -140, 35 -140, 35 -110, 20 -110, 20 -140))'::GEOMETRY);
INSERT INTO block_build_corr VALUES (1, 1), (2, 2), (3, 3), (4, 4), (4, 5), (5, 6), (6, 7), (4, 8), (7, 29), (8, 31),
(8, 32), (8, 33), (8, 34);
INSERT INTO rsu_test VALUES (1, 'POLYGON((0 0, 50 0, 50 40, 0 40, 0 0))'::GEOMETRY, 2000, 0.303, 0.937),
 (2, 'POLYGON((50 0, 55 0, 55 30, 50 30, 50 0))'::GEOMETRY, 150, 16.0/150, 2),
 (3, 'POLYGON((0 0, 0 -15, 25 -15, 25 0, 0 0))'::GEOMETRY, 375, 47.0/375, 0.304),
 (4, 'POLYGON((1000 1000, 1100 1000, 1100 1100, 1000 1100, 1000 1000))'::GEOMETRY, 10000, 0.4, null),
 (5, st_translate('POLYGON((1000 1000, 1100 1000, 1100 1100, 1000 1100, 1000 1000))'::GEOMETRY, 100, 0), 10000, 0.4, null),
 (6, st_translate('POLYGON((1000 1000, 1100 1000, 1100 1100, 1000 1100, 1000 1000))'::GEOMETRY, 200, 0), 10000, 0.4, null),
 (7, st_translate('POLYGON((1000 1000, 1100 1000, 1100 1100, 1000 1100, 1000 1000))'::GEOMETRY, 0, 100), 10000, 0.4, null),
 (8, st_translate('POLYGON((1000 1000, 1100 1000, 1100 1100, 1000 1100, 1000 1000))'::GEOMETRY, 100, 100), 10000, 0.4, null),
 (9, st_translate('POLYGON((1000 1000, 1100 1000, 1100 1100, 1000 1100, 1000 1000))'::GEOMETRY, 200, 100), 10000, 0.4, null),
 (10, st_translate('POLYGON((1000 1000, 1100 1000, 1100 1100, 1000 1100, 1000 1000))'::GEOMETRY, 0, 200), 10000, 0.4, null),
 (11, st_translate('POLYGON((1000 1000, 1100 1000, 1100 1100, 1000 1100, 1000 1000))'::GEOMETRY, 100, 200), 10000, 0.4, null),
 (12, st_translate('POLYGON((1000 1000, 1100 1000, 1100 1100, 1000 1100, 1000 1000))'::GEOMETRY, 200, 200), 10000, 0.4, null),
 (13, st_translate('POLYGON((1200 1200, 1300 1200, 1300 1300, 1200 1300, 1200 1200))'::GEOMETRY, 200, 200), 10000, 0.4, null),
 (14, 'POLYGON((0 100, 50 100, 50 150, 0 150, 0 100))'::GEOMETRY, 2500, 0, 0),
 (15, 'POLYGON((0 99, 50 99, 50 100, 0 100, 0 99))'::GEOMETRY, 50, 0, 0),
 (16, 'POLYGON((0 -150, 50 -150, 50 -100, 0 -100, 0 -150))'::GEOMETRY, 2500, 0, 0),
 (17, st_translate('POLYGON((1200 1200, 1300 1200, 1300 1300, 1200 1300, 1200 1200))'::GEOMETRY, 300, 300), 10000, 1.0, null);
INSERT INTO rsu_build_corr VALUES (1, 1, 10.178217821), (1, 2, 10.178217821), (1, 3, 10.178217821), (1, 4, 10.178217821),
 (1, 5, 10.178217821), (2, 6, 18), (3, 7, 3), (1, 8, 10.178217821), (4, 9, null), (4, 10, null), (5, 11, null), (5, 12, null),
 (6, 13, null), (6, 14, null), (7, 15, null), (7, 16, null), (8, 17, null), (8, 18, null), (9, 19, null), (9, 20, null),
 (10, 21, null), (10, 22, null), (11, 23, null), (11, 24, null), (12, 25, null), (12, 26, null), (13, 27, null),
 (13, 28, null), (3, 29, null);
INSERT INTO road_test VALUES (1, 'LINESTRING(120 60, 120 -10)'::GEOMETRY, 10, 0, null),
(2, 'LINESTRING (86 19, 170 20)'::GEOMETRY, 5, 0, null), (3, 'LINESTRING (93 53, 149 54, 145 -5)'::GEOMETRY, 5, 0, null),
(4, 'LINESTRING (85 60, 85 -1, 155 1, 148 54, 92 50, 96 -12, 119 -11, 117 -4, 78 -5)'::GEOMETRY, 10, 0, null),
(5, 'LINESTRING (20 100, 25 100, 25 120, 20 120)'::GEOMETRY, 6, 0, null),
(6, 'LINESTRING (50 105, 47 99)'::GEOMETRY, 6, -1, null);
INSERT INTO veget_test VALUES (1, 'POLYGON((35 98, 36 98, 36 104, 35 104, 35 98))'::GEOMETRY, 'low'),
(2, 'POLYGON((20 140, 25 140, 25 145, 20 145, 20 140))'::GEOMETRY, 'high'),
(3, 'POLYGON((45 130, 55 130, 55 135, 45 135, 45 130))'::GEOMETRY, 'high'),
(4, 'POLYGON((1000 1050, 1100 1050, 1100 1100, 1000 1100, 1000 1050))'::GEOMETRY, 'high'),
(5, 'POLYGON((1000 1000, 1050 1000, 1050 1100, 1000 1100, 1000 1000))'::GEOMETRY, 'low');
INSERT INTO hydro_test VALUES (1, 'POLYGON((-2 95, 2 95, 2 105, -2 105, -2 95))'::GEOMETRY),
(2, 'POLYGON((1050 1000, 1100 1000, 1100 1050, 1050 1050, 1050 1000))'::GEOMETRY);
INSERT INTO rsu_test_for_lcz VALUES (1, 0.3, 4, 0.5, 0.5, 0.05, 30, 8),
(2, 0.9, 0.4, 0.4, 0.45, 0.1, 5.5, 5);
