import os
import arcpy
from arcpy import env
from datetime import datetime
arcpy.CheckOutExtension("Spatial")

print 'Start time:', str(datetime.now())

# Buffer distance (ft)
buffer_dist = 30
buffer_dist = str(buffer_dist)
buffer_arg = buffer_dist + " feet"

#Folder and gdb where the output files will go
folder = r"C:\GIS\Water\Buffer_analysis"
gdb = buffer_dist + "_ft_buffers.gdb"
full_gdb = os.path.join(folder, gdb)

# Imports necessary files.
# Counties are just counties in CONUS.
# NHD is National Hydrography Dataset Plus V2.
# Land use is from Susan Cook-Patton at The Nature Conservancy
counties = r"C:\GIS\Multi-project\US_counties\tl_2017_us_county_reproj_World_Eckert_IV.shp"
NHD = r"C:\GIS\Multi-project\NHDPlusV2\NHDPlusNationalData\NHDPlusV2_National_Seamless.gdb\NHDSnapshot\NHDFlowline_Network"
land_use = r"C:\GIS\Water\Buffer_analysis\TNC_reforestation_area_from_Susan_Cook_20180515\Refor_To_Share.tif"

merged_file = 'Merged_buffers_' + buffer_dist +'_ft'
merged_file_full = os.path.join(full_gdb, merged_file)

merged_file_reproj = 'Merged_buffers_' + buffer_dist +'_ft_reproj'
merged_file_reproj_full = os.path.join(full_gdb, merged_file_reproj)

# #Creates a gdb for the output files
# arcpy.CreateFileGDB_management(folder, gdb)
#
# # Converts county and NHD shapefiles into layers
# arcpy.MakeFeatureLayer_management(counties, 'county_lyr')
# arcpy.MakeFeatureLayer_management(NHD, 'NHD_lyr')
#
# # To start the script at a particular feature
# where = '"FID" > 3104'
# arcpy.SelectLayerByAttribute_management('county_lyr', "NEW_SELECTION", where)
#
# # Coordinates for measuring area of vectors
# out_coords = arcpy.Describe(counties).spatialReference
#
# with arcpy.da.SearchCursor('county_lyr', ['GEOID', 'FID']) as cursor:
#     for row in cursor:
#
#         # County number, feature number
#         cnty = "{}".format(row[0])
#         FID = "{}".format(row[1])
#
#         print 'Processing county GEOID = ', cnty, 'feature number', FID
#
#         # Selects just the county being iterated through at the moment
#         where = '"GEOID" = ' + "'%s'" %cnty
#         arcpy.SelectLayerByAttribute_management('county_lyr', "NEW_SELECTION", where)
#
#         print '  County', cnty, 'selected. Now selecting intersecting NHD segements...'
#
#         # Selects all NHD segment that touch the county
#         arcpy.SelectLayerByLocation_management('NHD_lyr', 'INTERSECT', 'county_lyr')
#
#         print '  Intersecting NHD segments selected. Now selecting non-lake segments...'
#
#         # From above NHD segment selection, only selects the ones that aren't lakes/ponds/reservoirs
#         arcpy.SelectLayerByAttribute_management('NHD_lyr', 'SUBSET_SELECTION',
#                                                 '"WBAreaType" IN(\'Area of Complex Channels\', \'CanalDitch\', \'StreamRiver\', \'Wash\', \' \') ')
#
#         # The number of NHD segments in the county that fit the above criteria
#         NHD_count = arcpy.GetCount_management('NHD_lyr')
#         print '  Number of non-lake NHD segments in county', cnty,  ':', str(NHD_count)
#
#         # Where output files go
#         outpath = r"C:\GIS\Water\Buffer_analysis\County_buffers"
#
#         # Buffers NHD stream flowlines
#         print '  Buffering', cnty, 'at', buffer_arg
#         buffered_name = outpath + os.sep + cnty + "_buffer_" + buffer_dist + "ft.shp"
#         arcpy.Buffer_analysis('NHD_lyr', buffered_name, buffer_arg, "FULL", "ROUND", "ALL")
#
#         # Reprojects buffered flowlines to projection of the counties
#         print '  Reprojecting', buffer_arg, 'buffer for', cnty, 'to World Eckert IV'
#         reproj_name = outpath + os.sep + cnty + "_buffer_" + buffer_dist + "ft_reproj_ft.shp"
#         arcpy.Project_management(buffered_name, reproj_name, out_coords)
#
#         # Clips buffers to county
#         print '  Clipping', buffer_arg, 'buffers to', cnty
#         clipped_name = outpath + os.sep + cnty + "_buffer_" + buffer_dist + "ft_reproj_clip.shp"
#         arcpy.Clip_analysis(reproj_name, 'county_lyr', clipped_name)
#
#         # Calculates area in proper projection for buffered flowlines
#         print '  Calculating area for', cnty, 'segments with buffer of', buffer_arg
#         arcpy.AddGeometryAttributes_management(clipped_name, 'AREA', Area_Unit='ACRES')
#
#         arcpy.AddField_management(clipped_name, 'GEOID', 'TEXT')
#         arcpy.CalculateField_management(clipped_name, 'GEOID', '"' + cnty + '"', 'PYTHON')
#
#         arcpy.FeatureClassToGeodatabase_conversion(clipped_name, full_gdb)
#
#         # Deletes the intermediate shapefiles
#         print '  Deleting intermediate shapefiles for', cnty
#         arcpy.Delete_management(buffered_name)
#         arcpy.Delete_management(reproj_name)
#         arcpy.Delete_management(clipped_name)
#
#         print '  County end time:', str(datetime.now())
#
# print 'Done buffering counties at ' + str(datetime.now())

# # Gets all the feature classes containing county buffers in the gdb
# arcpy.env.workspace = full_gdb
# buffer_list = arcpy.ListFeatureClasses()
# print buffer_list
#
# # Merges the feature classes from each county into a single feature class containing all counties
# arcpy.Merge_management(buffer_list, merged_file_full)
# print 'Buffer files merged'

# # Projects the merged buffers
# print 'Projecting buffers to projection of land use raster'
# out_coords_land_use = arcpy.Describe(land_use).spatialReference
# arcpy.Project_management(merged_file_full, merged_file_reproj_full, out_coords_land_use)
#
# Calculates the area of each reforestable land use in each county buffer

print 'Calculating area of reforestable wetland in buffer'

wetland_table = os.path.join(full_gdb, 'Wetland_output_' + buffer_dist + '_ft')
# Wetland reforestable area (1 in field Wetland) is:
# '"Wetland" IN (1, 2) AND "NAFDforest" IN (0) AND "Urban" IN (0) AND "Roads" IN (0) AND "BPSforest" IN (1, 3)'
arcpy.MakeRasterLayer_management(land_use, "reforestable_wetland")
arcpy.sa.TabulateArea(merged_file_reproj_full, "GEOID", "reforestable_wetland", "Wetlnd", wetland_table)

# print 'Calculating area of pasture, crop, pasture/crop, and other reforestable land in buffer'
# pasture_crop_table= os.path.join(full_gdb, 'Pasture_crop_output_' + buffer_dist + '_ft')
# arcpy.sa.TabulateArea(merged_file_reproj_full, "GEOID", land_use, "RC", pasture_crop_table)

print 'Calculated reforestable land area'
#
# # Calculates the area of open water within the buffer
# print 'Calculating open water area'
# open_water_table= os.path.join(full_gdb, 'Open_water_output_' + buffer_dist + '_ft_new')
# arcpy.sa.TabulateArea(merged_file_reproj_full, "GEOID", land_use, "BPSgroupVe", open_water_table)
# print 'Calculated open water area'

print 'End time:', str(datetime.now())