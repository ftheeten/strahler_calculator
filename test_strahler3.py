#to run interactively : exec(open("./test_strahler3.py").read())
#import rasterio
import geopandas as gpd
import pandas as pnd
from osgeo import gdal
from osgeo import ogr
import numpy as np
from shapely.geometry import Point, Polygon

dem="C:\\DEV\\GIS\\Prefina\\petit_raster_srtm.tif"
rivers="C:\\DEV\\GIS\\Prefina\\petit_reseau_decompose_unique.gpkg"

def remove_self_intersection(df, list_name, index_name):
    df[list_name].remove(df[index_name])
    return df[list_name]

panda_rivs = gpd.read_file(rivers)
panda_rivs['idx_cpy'] = panda_rivs.index
print(len(panda_rivs))
#raster_data = rasterio.open(dem)

# Get raster geometry
raster = gdal.Open(dem)
array_altitude=raster.GetRasterBand(1).ReadAsArray()
transform = raster.GetGeoTransform()
pixelWidth = transform[1]
pixelHeight = transform[5]
cols = raster.RasterXSize
rows = raster.RasterYSize

xLeft = transform[0]
yTop = transform[3]
print(pixelWidth)
print(pixelHeight)
print(xLeft)
print(yTop)
print(rows)
print(cols)

start_x=xLeft
start_y=yTop

i=0
list_x_min=[]
list_y_min=[]
list_x_max=[]
list_y_max=[]
list_altitudes=[]
for y in range(0,rows):
    start_x=xLeft
    #height is negative when calculated from South!
    end_y=start_y-abs(pixelHeight)
    for x in range(0,cols):
        end_x=start_x+pixelWidth
        list_x_min.append(start_x)
        list_y_min.append(start_y)
        list_x_max.append(end_x)
        list_y_max.append(end_y)
        list_altitudes.append(array_altitude[y][x])
        start_x=end_x
        #print(i)
        i=i+1
    #print("----------------------------------------------------------------------------------------------")
    #print("old y="+str(start_y))
    start_y=end_y
    #print("new y="+str(start_y))

r_dict= {'start_x':list_x_min,'start_y':list_y_min, 'end_x':list_x_max,'end_y':list_y_max, 'altitude':list_altitudes}
tmp_panda=pnd.DataFrame(r_dict)
tmp_panda["wkt"]="POLYGON(("+tmp_panda['start_x'].map(str)+" "+tmp_panda['start_y'].map(str)+","+tmp_panda['end_x'].map(str)+" "+tmp_panda['start_y'].map(str)+","+tmp_panda['end_x'].map(str)+" "+tmp_panda['end_y'].map(str)+","+tmp_panda['start_x'].map(str)+" "+tmp_panda['end_y'].map(str)+","+tmp_panda['start_x'].map(str)+" "+tmp_panda['start_y'].map(str)+"))"
tmp_panda['coordinates'] = gpd.GeoSeries.from_wkt(tmp_panda['wkt'])

panda_raster = gpd.GeoDataFrame(tmp_panda, geometry='coordinates')
panda_raster = panda_raster.set_crs('epsg:4326')
print(panda_raster.head())

print("done")

#inter=panda_raster.overlay(panda_rivs, how='intersection', keep_geom_type=False)
inter=panda_rivs.overlay(panda_raster, how='intersection', keep_geom_type=False)
inter=inter[~inter.is_empty]
#print(inter)
print(inter.columns) 
#for index,row in panda_rivs.iterrows():
#    if(row["altitudes"]):
#        print(row["altitudes"])
#    inter=panda_raster.intersects(row['geometry'])
'''
for index,row in panda_raster.iterrows():
    inter=panda_rivs.intersects(row["coordinates"])
    flag=any(x == True for x in inter)
    if flag:
         print("inter")
'''
line_alt=inter[['idx_cpy', 'altitude']].groupby('idx_cpy').agg({'altitude':lambda x: list(x)})


panda_rivs = panda_rivs.merge(line_alt, on="idx_cpy", how = 'inner')
panda_rivs["min_alt"]=panda_rivs["altitude"].apply(lambda x: int(min(x)))
panda_rivs["max_alt"]=panda_rivs["altitude"].apply(lambda x: int(max(x)))
panda_rivs["strahler"]=0
panda_rivs["length"]=panda_rivs['geometry'].length
panda_rivs["cumul_length"]=0
panda_rivs["source"]=""
panda_rivs["checked"]=False
panda_rivs["epoch"]=0
panda_rivs["path"]=[]
print(panda_rivs)

max_alt=panda_rivs["max_alt"].max()
min_alt=panda_rivs["max_alt"].min()


#for z in range(max_alt, min_alt-1, -1):
#    print(z)
    
neigh=panda_rivs.overlay(panda_rivs, how='intersection', keep_geom_type=False)
list_neigh=neigh[['idx_cpy_1', 'idx_cpy_2']].groupby('idx_cpy_1').agg({'idx_cpy_2':lambda x: list(x)})
list_neigh.rename(columns={ list_neigh.columns[0]: "idx_cpy_2" }, inplace = True)
list_neigh['idx_cpy'] = list_neigh.index
list_neigh['idx_cpy_2']=list_neigh.apply(lambda x: remove_self_intersection(x,'idx_cpy_2','idx_cpy' ), axis=1)
panda_rivs = panda_rivs.merge(list_neigh, on="idx_cpy", how = 'inner')

panda_rivs["nb_neighbours"]=panda_rivs["idx_cpy_2"].apply(lambda x: len(x))

epoch=1
start=panda_rivs[panda_rivs['nb_neighbours']<=1]
#repérer les sources et les flager à True
for z in range(max_alt, min_alt-1, -1):
    print(z)
    spring=start[start["max_alt"]==z]
    if spring is not None:
        if len(spring)>0:
            #idx_cpy=spring.iloc[0]["idx_cpy"]
            #print("idx: "+str(idx_cpy))
            for index, row in spring.iterrows():
                panda_rivs.loc[panda_rivs["idx_cpy"]==row["idx_cpy"], "checked"]=True
                panda_rivs.loc[panda_rivs["idx_cpy"]==row["idx_cpy"], "strahler"]=1
                panda_rivs.loc[panda_rivs["idx_cpy"]==row["idx_cpy"], "source"]=panda_rivs["idx_cpy"]
                panda_rivs.loc[panda_rivs["idx_cpy"]==row["idx_cpy"],"cumul_length"]=panda_rivs.loc[panda_rivs["idx_cpy"]==row["idx_cpy"],"length"]
                panda_rivs.loc[panda_rivs["idx_cpy"]==row["idx_cpy"],"epoch"]=epoch
                path_tmp=panda_rivs.loc[panda_rivs["idx_cpy"]==row["idx_cpy"],"path"]
                path_tmp.append(row["idx_cpy"])
                panda_rivs.loc[panda_rivs["idx_cpy"]==row["idx_cpy"],"path"]=path_tmp
                #add downstram
 
go=True
len_checked=len(panda_rivs[panda_rivs['checked']==True]) 
while go:
    epoch=epoch+1
    #dernier jeu de données
    last_set=panda_rivs.loc[panda_rivs["checked"]==True and panda_rivs["epoch"]=epoch-1]
    #pas encore vérifiés
    #to_compare=panda_rivs.loc[panda_rivs["checked"]==False]
    for index, row in last_set.iterrows():
        find_neigh=panda_rivs.overlay(row['geometry'], how='intersection', keep_geom_type=False)
        if find_neigh is not None:
            max_strahler=find_neigh["strahler"].max()
            if len(find_neigh[find_neigh["strahler"]==max_strahler]) >0:
                max_strahler=max_strahler+1
            '''
            if len(find_neigh)==1:
                    panda_rivs.loc[panda_rivs["idx_cpy"]==find_neigh.iloc[0]["idx_cpy"], "strahler"]=find_neigh.iloc[0]["strahler"]
            elif len(find_neigh)>1:
                max_strahler=0
                for index2, row2 in find_neigh.iterrows():
                '''
                #    list_strahler=
                    '''
                    if row2["checked"]==False:
                        panda_rivs.loc[panda_rivs["idx_cpy"]==row2["idx_cpy"], "strahler"]=True
                    elif row2["True"]==True:
                    '''
            '''
            if len(find_neigh)==1:
                if find_neigh.iloc[0][checked]==False:
                
                else:
            elif  len(find_neigh)>1:
            '''
        '''
        current_strahler=row['strahler']
        find_neigh=to_compare.overlay(row['geometry'], how='intersection', keep_geom_type=False)
        if find_neigh is not None:
            if len(find_neigh)>0:
                max_strahler=find_neigh["strahler"].apply(lambda x: int(min(x)))
                current_strahler=max(max_strahler, current_strahler)
                for index2, row2 in find_neigh.iterrows():
                    panda_rivs.loc[row2["idx_cpy"]==row["idx_cpy"], "checked"]=True
                    panda_rivs.loc[row2["idx_cpy"]==row["idx_cpy"], "strahler"]=current_strahler+len(find_neigh)-1
        '''
    len_checked2=len(panda_rivs[panda_rivs['checked']==True])
    if len_checked2== len_checked:
        go=False
    