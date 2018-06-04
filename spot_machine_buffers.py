# Attempt to get stream buffers of various distances for each county in the conterminous US,
# then get the areas of select land uses in each of them
# with the goal of estimating reforestation potential in stream buffers of various widths by county.
# Done using PostGIS calls from Python.
# I got the buffers of various widths generated for a single county (didn't try iterating buffer generation for all counties)
# on my local computer.
# However, I couldn't get the land use raster to line up with the buffer feature classes, so I was never able to calculate the
# area of each land use in each buffer.
# I was able to check the spatial alignment of files and their appearance in QGIS, which I connected to the PostGIS database
# I was working with.
# I also tried getting the buffer creation working on a spot machine but was stymied on that, too. I got the county
# boundary shapefile loaded onto the spot machine and unzipped, and presumably operable.
# However, I couldn't get the NHD geodatabase onto the spot machine. I couldn't transfer it unzipped to the spot machine
# and I couldn't properly unzip the zipped version I did get to the spot machine.
# Thus, I couldn't get NHD working on the spot machine.
# Unable to get the buffering working on the spot machine (because I couldn't get NHD loaded) and unable to get final
# land use area results on the local computer (because I couldn't align the buffers with the land use raster)
# I stopped working on this approach to the problem.
# I'd already gotten an answer using ArcPy, after all.
# David Gibbs
# May/June 2018

import psycopg2
import subprocess
import os

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

# Paths for land use file on a local machine
landuse_path = 'C:\\GIS\\Water\\Buffer_analysis\\TNC_refor_raster_clipped_to_Fulton_Cnty_20180529.tif'

# Names of county and NHD files imported into PostGIS
counties = "us_counties_reproj"
NHD = "NHD_streams"

# PostGIS database host
host="localhost"
host_name= 'PG:host={}'.format(host)

# # Copies county shapefile from S3 to spot machine and unzips it.
# # Only necessary if running on a spot machine.
# print 'GETTING COUNTIES'
# copy_counties = ['aws', 's3', 'cp', 's3://gfw-files/dgibbs/Multi_project/US_counties/tl_2017_us_county_reproj_World_Eckert_IV.zip', './Input_files']
# print " ".join(copy_counties)
# subprocess.check_call(copy_counties)
# print 'COUNTIES DOWNLOADED '
# zip_ref = zipfile.ZipFile('./Input_files/tl_2017_us_county_reproj_World_Eckert_IV.zip', 'r')
# zip_ref.extractall('./Input_files')
# zip_ref.close()
# print 'COUNTIES UNZIPPED'
#
# # Copies NHD from S3 to spot machine and unzips it
# # Only necessary if running on a spot machine.
# print 'GETTING NHD'
# copy_nhd = ['aws', 's3', 'cp', 's3://gfw-files/dgibbs/Multi_project/NHD/NHDPlusV2_National_Seamless.gdb.zip', './Input_files']
# print " ".join(copy_nhd)
# subprocess.check_call(copy_nhd)
# print 'NHD DOWNLOADED'
# subprocess.check_call('sudo apt install p7zip-full')                  # Tested this on the spot machine command line but not from Python. Installed from spot machine command line.
# subprocess.check_call('7z e NHDPlusV2_National_Seamless.gdb.zip')     # Tested this on the spot machine command line but not from Puthon. Unzipped from spot machine command line but in the folder the zip was in, so it didn't get me fully there.
# print 'NHD UNZIPPED'

# Builds connection string
conn = psycopg2.connect(host=host)

# Creates a cursor object
curs = conn.cursor()

# Commands for importing county boundaries, NHD flowlines, and land use files to Postgres database
county_upload = ['ogr2ogr', '-f', 'PostgreSQL', host_name,
                county_path_local,                              # change this to county_path_spot if running on a spot machine
                '-overwrite', '-progress',
                '-nln', counties,
                '-nlt', 'PROMOTE_TO_MULTI',
                '-select', 'STATEFP, COUNTYFP, GEOID, NAME'
                , '-t_srs', 'EPSG:54012'
                , '-sql', 'SELECT * from tl_2017_us_county_reproj_world_eckert_iv WHERE GEOID IN (\'13121\')'
                ]

nhd_upload = ['ogr2ogr', '-f', 'PostgreSQL', host_name,
                nhd_path_local,                                 # change this to county_path_spot if running on a spot machine
                nhd_file, '-overwrite', '-progress',
                '-select', 'COMID, StreamOrde, FTYPE, FCODE, WBAreaType',
                '-sql', 'SELECT * from NHDFlowline_Network WHERE WBAreaType IN (\'Area of Complex Channels\', \'CanalDitch\', \'StreamRiver\', \'Wash\', \' \')',
                '-nln', NHD, '-dim', '2'
                , '-t_srs', 'EPSG:54012'
                ]

LU_upload = 'raster2pgsql -d -I -C -M -s 6703 {LU} fulton_LU | psql'.format(LU=landuse_path)

# Actually runs the import commands
print 'IMPORTING COUNTIES TO DATABASE'
print " ".join(county_upload)
subprocess.check_call(county_upload)
print 'IMPORTING NHD TO DATABASE'
print " ".join(nhd_upload)
subprocess.check_call(nhd_upload)
print " ".join(LU_upload)
subprocess.call(LU_upload, shell=True)

area_field = "area_sqmtr"

# Command to clip the NHD streams to the county of interest
clip = ('CREATE TABLE nhd_clip AS '
        'SELECT ST_Intersection(c.wkb_geometry, n.wkb_geometry) AS geom, '
        'c.STATEFP, c.COUNTYFP, c.GEOID, c.NAME, '
        'n.comid, n.StreamOrde, n.FTYPE, n.FCODE, n.WBAreaType '
        'FROM {c} c, {n} n '
        'WHERE ST_Intersects(c.wkb_geometry, n.wkb_geometry); '.format(c=counties, n=NHD))

print 'Clipping streams to county'
curs.execute(clip)

# Range sets how many stream buffers there will be. They are 5 ft. apart.
for x in range(1, 7):

    # Sets the buffer distance 5 ft. apart in terms of meters
    distance = x * 3.048/2
    final_table = "nhd_clip_buff_clip_dissolve_" + str(x*5) + "_ft"

    # Command to buffer the NHD segments within the county of interest
    buffer = ('CREATE TABLE nhd_clip_buff AS '
            'SELECT ST_Buffer(n.geom, {d}) AS geom, '
                'n.comid, n.StreamOrde, n.FTYPE, n.FCODE, n.WBAreaType '
            'FROM nhd_clip n; '.format(d=distance))

    #Clips the buffers back to the county of interest
    reclip = ('CREATE TABLE nhd_clip_buff_clip AS '
            'SELECT ST_Intersection(c.wkb_geometry, n.geom) AS geom, '
                'c.STATEFP, c.COUNTYFP, c.GEOID, c.NAME, '
                'n.comid, n.StreamOrde, n.FTYPE, n.FCODE, n.WBAreaType '
            'FROM {c} c, nhd_clip_buff n '
            'WHERE ST_Intersects(c.wkb_geometry, n.geom); '.format(c=counties))

    # Combines all the buffers in the county into a single buffer
    dissolve = ('DROP TABLE IF EXISTS {t}; '
            'CREATE TABLE {t} AS '
            'SELECT ST_Union(n.geom) AS geom, '
                'n.GEOID '
            'FROM nhd_clip_buff_clip n '
            'GROUP BY n.GEOID; '.format(t=final_table))

    # Calculates the area of the buffer in the county
    area = ('ALTER TABLE {t} ADD COLUMN {a} real; '
            'UPDATE {t} SET {a} = ST_Area(geom); '.format(t=final_table, a=area_field))

    project = ('ALTER TABLE {t} '
            'ALTER COLUMN geom '
            'TYPE geometry(Geometry, 6703) '
            'USING ST_Transform(geom, 6703); '.format(t=final_table))

    delete = ('DROP TABLE nhd_clip_buff; '
              'DROP TABLE nhd_clip_buff_clip; ')

    # Actually runs the commands in SQL
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

# Deletes the initial table for the county
final_delete = ('DROP TABLE nhd_clip; ')
curs.execute(final_delete)

# Commits changes to Postgres database
conn.commit()

# Closes the connection to the Postgres database
conn.close()