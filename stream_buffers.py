import os
import arcpy
from arcpy import env
from datetime import datetime

# env.workspace = r"C:\GIS\Water"

print 'Start time:', str(datetime.now())

gdb = r"C:\GIS\Water\Buffer_analysis\30_ft_buffers.gdb"

# # Imports necessary files.
# # Counties are just counties in CONUS.
# # NHD is National Hydrography Dataset Plus V2.
# counties = r"C:\GIS\Multi-project\US counties\tl_2017_us_county_reproj_World_Eckert_IV.shp"
# NHD = r"C:\GIS\Multi-project\NHDPlusV2\NHDPlusNationalData\NHDPlusV2_National_Seamless.gdb\NHDSnapshot\NHDFlowline_Network"
#
# # Converts county and NHD shapefiles into layers
# arcpy.MakeFeatureLayer_management(counties, 'county_lyr')
# arcpy.MakeFeatureLayer_management(NHD, 'NHD_lyr')
#
# ## To start the script at a particular feature
# # where = '"FID" = 1366'
# # arcpy.SelectLayerByAttribute_management('county_lyr', "NEW_SELECTION", where)
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
#         for x in range(6, 7):
#
#             # Buffer distance
#             buffer_dist = x * 5
#             buffer_dist = str(buffer_dist)
#             buffer_arg = buffer_dist + " feet"
#
#             # Buffers NHD stream flowlines
#             print '  Buffering', cnty, 'at', buffer_arg
#             buffered_name = outpath + os.sep + cnty + "_buffer_" + buffer_dist + "ft.shp"
#             arcpy.Buffer_analysis('NHD_lyr', buffered_name, buffer_arg, "FULL", "ROUND", "ALL")
#
#             # Reprojects buffered flowlines to projection of the counties
#             print '  Reprojecting', buffer_arg, 'buffer for', cnty, 'to World Eckert IV'
#             reproj_name = outpath +os.sep +cnty + "_buffer_" + buffer_dist + "ft_reproj_ft.shp"
#             arcpy.Project_management(buffered_name, reproj_name, out_coords)
#
#             # Clips buffers to county
#             print '  Clipping', buffer_arg, 'buffers to', cnty
#             clipped_name = outpath + os.sep + cnty + "_buffer_" + buffer_dist + "ft_reproj_clip.shp"
#             arcpy.Clip_analysis(reproj_name, 'county_lyr', clipped_name)
#
#             # Calculates area in proper projection for buffered flowlines
#             print '  Calculating area for', cnty, 'segments with buffer of', buffer_arg
#             arcpy.AddGeometryAttributes_management(clipped_name, 'AREA', Area_Unit='ACRES')
#
#             arcpy.AddField_management(clipped_name, 'GEOID', 'TEXT')
#             arcpy.CalculateField_management(clipped_name, 'GEOID', '"' + cnty + '"', 'PYTHON')
#
#             arcpy.FeatureClassToGeodatabase_conversion(clipped_name, gdb)
#
#             # Deletes the intermediate shapefiles
#             print '  Deleting intermediate shapefiles for', cnty
#             arcpy.Delete_management(buffered_name)
#             arcpy.Delete_management(reproj_name)
#             arcpy.Delete_management(clipped_name)
#
#             print '  County end time:', str(datetime.now())
#
#
# print 'End time:', str(datetime.now())



arcpy.env.workspace = r"C:\GIS\Water\Buffer_analysis\30_ft_buffers.gdb"
buffer_list = arcpy.ListFeatureClasses()

print buffer_list

arcpy.Merge_management(buffer_list, os.path.join(gdb, 'Merged_buffer_shps_30_ft'))
print 'Buffer files merged'
