# connects to postgres
import psycopg2
import subprocess
import os
import zipfile

# Paths for NHD flowlines if run on a local machine or a spot machine
nhd_spot_folder = 'Input_files'
nhd_local_folder = 'C:\\GIS\\Multi_project\\NHDPlusV2\\NHDPlusNationalData\\NHDPlusV2_National_Seamless.gdb'
nhd_gdb = 'NHDPlusV2_National_Seamless.gdb'
nhd_file = 'NHDFlowline_Network'
nhd_path_spot = os.path.join(nhd_spot_folder, nhd_gdb)
nhd_path_local = os.path.join(nhd_local_folder)

# Paths for county boundaries if run on a local machine or a spot machine
county_file = 'tl_2017_us_county_reproj_World_Eckert_IV.shp'
county_spot = 'Input_files'
county_local = 'C:\\GIS\\Multi_project\\US_counties'
county_path_spot = os.path.join(county_spot, county_file)
county_path_local = os.path.join(county_local, county_file)

# Paths for land use file
landuse_path = 'C:\\GIS\\Water\\Buffer_analysis\\TNC_refor_raster_clipped_to_Fulton_Cnty_20180529.tif'

# Names of county and NHD files imported into PostGIS
counties = "us_counties_reproj"
NHD = "NHD_streams"

# PostGIS database host
host="localhost"
host_name= 'PG:host={}'.format(host)

copy_counties = ['aws', 's3', 'cp', 's3://gfw-files/dgibbs/Multi_project/US_counties/tl_2017_us_county_reproj_World_Eckert_IV.zip', './Input_files']
print " ".join(copy_counties)
subprocess.check_call(copy_counties)
zip_ref = zipfile.ZipFile('./Input_files/tl_2017_us_county_reproj_World_Eckert_IV.zip', 'r')
zip_ref.extractall('./Input_files')
zip_ref.close()

copy_nhd = ['aws', 's3', 'cp', 's3://gfw-files/dgibbs/Multi_project/NHDPlusV2_National_Seamless.gdb', './Input_files']
print " ".join(copy_nhd)
subprocess.check_call(copy_nhd)
# zip_ref = zipfile.ZipFile('./Input_files/tl_2017_us_county_reproj_World_Eckert_IV.zip', 'r')
# zip_ref.extractall('./Input_files')
# zip_ref.close()

# builds our connection string, just like for ogr2ogr
conn = psycopg2.connect(host=host)

# creates a cursor object
curs = conn.cursor()

# Commands for importing county boundaries, NHD flowlines, and land use files to Postgres database
county_upload = ['ogr2ogr', '-f', 'PostgreSQL', host_name,
                county_path_spot, '-overwrite', '-progress',
                '-nln', counties,
                '-nlt', 'PROMOTE_TO_MULTI',
                '-select', 'STATEFP, COUNTYFP, GEOID, NAME'
                # , '-t_srs', 'EPSG:54012'
                , '-sql', 'SELECT * from tl_2017_us_county_reproj_world_eckert_iv WHERE GEOID IN (\'13121\')'
                ]

nhd_upload = ['ogr2ogr', '-f', 'PostgreSQL', host_name,
                nhd_path_spot, nhd_file, '-overwrite', '-progress',
                '-select', 'COMID, StreamOrde, FTYPE, FCODE, WBAreaType',
                '-sql', 'SELECT * from NHDFlowline_Network WHERE WBAreaType IN (\'Area of Complex Channels\', \'CanalDitch\', \'StreamRiver\', \'Wash\', \' \')',
                '-nln', NHD, '-t_srs', 'EPSG:54012', '-dim', '2'
                ]

LU_upload = 'raster2pgsql -d -I -C -M -s 6703 {LU} fulton_LU | psql'.format(LU=landuse_path)

# Actually runs the import commands
print " ".join(county_upload)
subprocess.check_call(county_upload)
print " ".join(nhd_upload)
subprocess.check_call(nhd_upload)
# print " ".join(LU_upload)
# subprocess.call(LU_upload, shell=True)
#
# area_field = "area_sqmtr"
#
# clip = ('CREATE TABLE nhd_clip AS '
#         'SELECT ST_Intersection(c.wkb_geometry, n.wkb_geometry) AS geom, '
#         'c.STATEFP, c.COUNTYFP, c.GEOID, c.NAME, '
#         'n.comid, n.StreamOrde, n.FTYPE, n.FCODE, n.WBAreaType '
#         'FROM {c} c, {n} n '
#         'WHERE ST_Intersects(c.wkb_geometry, n.wkb_geometry); '.format(c=counties, n=NHD))
#
# print 'Clipping streams to county'
# curs.execute(clip)
#
# for x in range(1, 3):
#
#     distance = x * 3.048/2
#     final_table = "nhd_clip_buff_clip_dissolve_" + str(x*5) + "_ft"
#
#     buffer = ('CREATE TABLE nhd_clip_buff AS '
#             'SELECT ST_Buffer(n.geom, {d}) AS geom, '
#                 'n.comid, n.StreamOrde, n.FTYPE, n.FCODE, n.WBAreaType '
#             'FROM nhd_clip n; '.format(d=distance))
#
#     reclip = ('CREATE TABLE nhd_clip_buff_clip AS '
#             'SELECT ST_Intersection(c.wkb_geometry, n.geom) AS geom, '
#                 'c.STATEFP, c.COUNTYFP, c.GEOID, c.NAME, '
#                 'n.comid, n.StreamOrde, n.FTYPE, n.FCODE, n.WBAreaType '
#             'FROM {c} c, nhd_clip_buff n '
#             'WHERE ST_Intersects(c.wkb_geometry, n.geom); '.format(c=counties))
#
#     dissolve = ('DROP TABLE IF EXISTS {t}; '
#             'CREATE TABLE {t} AS '
#             'SELECT ST_Union(n.geom) AS geom, '
#                 'n.GEOID '
#             'FROM nhd_clip_buff_clip n '
#             'GROUP BY n.GEOID; '.format(t=final_table))
#
#     area = ('ALTER TABLE {t} ADD COLUMN {a} real; '
#             'UPDATE {t} SET {a} = ST_Area(geom); '.format(t=final_table, a=area_field))
#
#     project = ('ALTER TABLE {t} '
#             'ALTER COLUMN geom '
#             'TYPE geometry(Geometry, 6703) '
#             'USING ST_Transform(geom, 6703); '.format(t=final_table))
#
#     delete = ('DROP TABLE nhd_clip_buff; '
#               'DROP TABLE nhd_clip_buff_clip; ')
#
#     print 'Buffering clipped streams at', str(x*5), 'ft.'
#     curs.execute(buffer)
#     print 'Clipping buffered streams'
#     curs.execute(reclip)
#     print 'Combining buffer segments into single buffer for county'
#     curs.execute(dissolve)
#     print 'Calculating buffer area'
#     curs.execute(area)
#     print 'Projecting buffer to land use raster'
#     curs.execute(project)
#     print 'Deleting intermediate tables'
#     curs.execute(delete)
#
# final_delete = ('DROP TABLE nhd_clip; ')
# curs.execute(final_delete)

# Commits changes to Postgres database
conn.commit()

# Closes the connection to the Postgres database
conn.close()