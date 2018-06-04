# connects to postgres
import psycopg2
import subprocess
import os
# import boto3

# For local use
# Paths for NHD flowlines, county boundaries, and land use file
nhd_dir = 'C:\\GIS\\Multi-project\\NHDPlusV2\\NHDPlusNationalData\\NHDPlusV2_National_Seamless.gdb'
nhd_lines = 'NHDFlowline_Network'
nhd_path = os.path.join(nhd_dir, nhd_lines)
county_path = 'C:\\GIS\\Multi-project\\US_counties\\tl_2017_us_county_reproj_world_eckert_iv.shp'
landuse_path = 'C:\\GIS\\Water\\Buffer_analysis\\TNC_refor_raster_clipped_to_Fulton_Cnty_20180529.tif'

counties = "us_counties_reproj"

# host="localhost"
host="54.147.182.79"
host_name= 'PG:host={}'.format(host)

copy_files = ['aws', 's3', 'cp', 's3://gfw-files/dgibbs/Multi_project/US_counties/tl_2017_us_county_reproj_World_Eckert_IV.shp', '.']

# aws s3 cp s3://gfw-files/dgibbs/Multi_project/US_counties/tl_2017_us_county_reproj_World_Eckert_IV.shp C:/GIS/test.shp

print " ".join(copy_files)
subprocess.check_call(copy_files)

# create_user = ['CREATE USER', '\"david.gibbs\"']
# create_db = ['CREATE DATABASE', '\"david.gibbs\"', 'OWNER', '\"david.gibbs\"']
#
# print " ".join(create_user)
# subprocess.check_call(create_user)
# print " ".join(create_db)
# subprocess.check_call(create_db)

# builds our connection string, just like for ogr2ogr
conn = psycopg2.connect(host=host)

# creates a cursor object
curs = conn.cursor()

# Commands for importing county boundaries, NHD flowlines, and land use files to Postgres database
county_upload = ['ogr2ogr', '-f', 'PostgreSQL', host_name,
                county_path, '-overwrite', '-progress',
                '-nln', counties,
                '-nlt', 'PROMOTE_TO_MULTI',
                '-select', 'STATEFP, COUNTYFP, GEOID, NAME'
                , '-t_srs', 'EPSG:54012'
                , '-sql', 'SELECT * from tl_2017_us_county_reproj_world_eckert_iv WHERE GEOID IN (\'13121\')'
                ]

nhd_upload = ['ogr2ogr', '-f', 'PostgreSQL', host_name,
                nhd_dir, nhd_lines, '-overwrite', '-progress',
                '-select', 'COMID, StreamOrde, FTYPE, FCODE, WBAreaType',
                '-sql', 'SELECT * from NHDFlowline_Network WHERE WBAreaType IN (\'Area of Complex Channels\', \'CanalDitch\', \'StreamRiver\', \'Wash\', \' \')',
                '-nln', 'NHD_streams_new', '-t_srs', 'EPSG:54012', '-dim', '2'
                ]

LU_upload = 'raster2pgsql -d -I -C -M -s 6703 {LU} fulton_LU | psql'.format(LU=landuse_path)

# Actually runs the import commands
print " ".join(county_upload)
subprocess.check_call(county_upload)
print " ".join(nhd_upload)
subprocess.check_call(nhd_upload)
print " ".join(LU_upload)
subprocess.call(LU_upload, shell=True)

county = counties
nhd_streams = "NHD_streams_new"
area_field = "area_sqmtr"

clip = ('CREATE TABLE nhd_clip AS '
        'SELECT ST_Intersection(c.wkb_geometry, n.wkb_geometry) AS geom, '
        'c.STATEFP, c.COUNTYFP, c.GEOID, c.NAME, '
        'n.comid, n.StreamOrde, n.FTYPE, n.FCODE, n.WBAreaType '
        'FROM {c} c, {n} n '
        'WHERE ST_Intersects(c.wkb_geometry, n.wkb_geometry); '.format(c=county, n=nhd_streams))

print 'Clipping streams to county'
curs.execute(clip)

for x in range(1, 3):

    distance = x * 3.048/2
    final_table = "nhd_clip_buff_clip_dissolve_" + str(x*5) + "_ft"

    buffer = ('CREATE TABLE nhd_clip_buff AS '
            'SELECT ST_Buffer(n.geom, {d}) AS geom, '
                'n.comid, n.StreamOrde, n.FTYPE, n.FCODE, n.WBAreaType '
            'FROM nhd_clip n; '.format(d=distance))

    reclip = ('CREATE TABLE nhd_clip_buff_clip AS '
            'SELECT ST_Intersection(c.wkb_geometry, n.geom) AS geom, '
                'c.STATEFP, c.COUNTYFP, c.GEOID, c.NAME, '
                'n.comid, n.StreamOrde, n.FTYPE, n.FCODE, n.WBAreaType '
            'FROM {c} c, nhd_clip_buff n '
            'WHERE ST_Intersects(c.wkb_geometry, n.geom); '.format(c=county))

    dissolve = ('DROP TABLE IF EXISTS {t}; '
            'CREATE TABLE {t} AS '
            'SELECT ST_Union(n.geom) AS geom, '
                'n.GEOID '
            'FROM nhd_clip_buff_clip n '
            'GROUP BY n.GEOID; '.format(t=final_table))

    area = ('ALTER TABLE {t} ADD COLUMN {a} real; '
            'UPDATE {t} SET {a} = ST_Area(geom); '.format(t=final_table, a=area_field))

    project = ('ALTER TABLE {t} '
            'ALTER COLUMN geom '
            'TYPE geometry(Geometry, 6703) '
            'USING ST_Transform(geom, 6703); '.format(t=final_table))

    delete = ('DROP TABLE nhd_clip_buff; '
              'DROP TABLE nhd_clip_buff_clip; ')

    print 'Buffering clipped streams at', str(x*5), 'ft.'
    curs.execute(buffer)
    print 'Clipping buffered streams'
    curs.execute(reclip)
    print 'Combining buffer segments into single buffer for county'
    curs.execute(dissolve)
    print 'Calculating buffer area'
    curs.execute(area)
    print 'Projecting buffer to land use raster'
    curs.execute(project)
    print 'Deleting intermediate tables'
    curs.execute(delete)

final_delete = ('DROP TABLE nhd_clip; ')
curs.execute(final_delete)

# Commits changes to Postgres database
conn.commit()

# Closes the connection to the Postgres database
conn.close()