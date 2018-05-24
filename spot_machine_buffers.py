# connects to postgres
import psycopg2

# builds our connection string, just like for ogr2ogr
conn = psycopg2.connect(host="localhost", dbname="david.gibbs")

# creates a cursor object
curs = conn.cursor()

county = "fulton_county_reproj"
nhd_streams = "nhd_streams"
area_field = "area_sqmtr"

clip = ('CREATE TABLE nhd_clip AS '
        'SELECT ST_Intersection(c.wkb_geometry, n.wkb_geometry) AS geom, '
        'c.STATEFP, c.COUNTYFP, c.GEOID, c.NAME, '
        'n.comid, n.StreamOrde, n.FTYPE, n.FCODE, n.WBAreaType '
        'FROM {c} c, {n} n '
        'WHERE ST_Intersects(c.wkb_geometry, n.wkb_geometry); '.format(c=county, n=nhd_streams))

print 'Clipping streams to county'
curs.execute(clip)

for x in range(1, 7):

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

    delete = ('DROP TABLE nhd_clip; '
              'DROP TABLE nhd_clip_buff; '
              'DROP TABLE nhd_clip_buff_clip; ')

    print 'Buffering clipped streams'
    curs.execute(buffer)
    print 'Clipping buffered streams'
    curs.execute(reclip)
    print 'Combining buffer segments into single buffer for county'
    curs.execute(dissolve)
    print 'Calculating buffer area'
    curs.execute(area)
    print 'Deleting intermediate tables'
    curs.execute(delete)


# and then commit our changes
# very important or they won't show up in the DB!
conn.commit()

# and then close our connection
conn.close()