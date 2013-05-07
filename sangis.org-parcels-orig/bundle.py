'''
'''

from  databundles.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
 
    def meta(self):
        
        # To generate the schema, must have the table already loaded
        with open (self.filesystem.path('meta','orig_schema.csv'), 'wb') as f:
            self.schema.as_csv(f)
        
        return True
 
    def build(self):
        from databundles.identity import PartitionIdentity

        for name, url in self.config.build.sources.items():
            pid = PartitionIdentity(self.identity, table=name+'_orig')
            p = self.partitions.new_geo_partition(pid, shape_file=url)


        self.add_indexes()

        self.split_docs()
        
        self.add_places()
        

        return True

    def split_docs(self):
        """Split the original parcel records into parcels, with a few extra fields, and parcel docs, 
        with just the situs and assessors information"""
        
        parcels = self.partitions.find_or_new_geo(table='parcels', space='all')
        parceldocs = self.partitions.find_or_new(table='parceldocs', space='all')
        parcels_o = self.partitions.find(table='parcels_orig')
        
        lr = self.init_log_rate(2000)
        
        p_ins =  parcels.database.inserter()
        pd_ins =  parceldocs.database.inserter()
                
        for row in parcels_o.query("""
            SELECT *, 
            shape_area AS area,
            x_coord AS x, y_coord AS y,
            X(Transform(Centroid(geometry), 4326)) AS lon, Y(Transform(Centroid(geometry), 4326)) AS lat,
            AsText(geometry) as wkt
            FROM parcels_orig
        """):
        
            row = dict(row)
        
            lr("Copy parcels")
            
            p_ins.insert(row)
            pd_ins.insert(row)            
        

    def add_places(self):      
        from databundles.geo.util import segment_points
        
        parcels = self.partitions.find_or_new(table='parcels', space='all')
        
        # Spatialite takes over the primary key
        #parcels.query("UPDATE parcels SET parcels_id = objectid");
        #parcels.query('CREATE INDEX IF NOT EXISTS parcels_id_idx ON parcels (parcels_id);')
        
        lr = self.init_log_rate(1000)
        
        _, places = self.library.dep('places')
        
        for area, where, is_in in segment_points(places, 
                                                 "SELECT *, AsText(geometry) AS wkt FROM places"):

            count = 0;

            print where
            with parcels.database.updater() as upd:
                for parcel in parcels.query("SELECT *, AsText(geometry) as wkt FROM parcels WHERE {}".format(where)):
                    count +=1
                    
                    parcel = dict(parcel)

                    if is_in(parcel['lon'], parcel['lat']):
                        lr("Update {} {}".format(area['type'], area['name']))
                        upd.update({'_OGC_FID': parcel['OGC_FID'],
                               '_'+area['type'] : area['code']
                               })


            self.log("{} {}: {}".format(area['type'], area['name'], count))

    def add_indexes(self):

        p = self.partitions.find(table='parcels_orig')
        p.database.query('CREATE INDEX IF NOT EXISTS apn_idx ON parcels_orig (apn, apn_8);')
        p.database.query('CREATE INDEX IF NOT EXISTS parcel_idx ON parcels_orig (parcelid);')
        p.database.query('CREATE INDEX IF NOT EXISTS road_idx ON parcels_orig (addrno, roadname);')
        p.database.query('CREATE INDEX IF NOT EXISTS sroad_idx ON parcels_orig (situs_addr, situs_stre);')
        p.database.query('CREATE INDEX IF NOT EXISTS zip_idx ON parcels_orig (zip);')
        p.database.query('CREATE INDEX IF NOT EXISTS szip_idx ON parcels_orig (situs_zip);')
        p.database.query('CREATE INDEX IF NOT EXISTS jur_idx ON parcels_orig (situs_juri);')
        p.database.query('CREATE INDEX IF NOT EXISTS coord_idx ON parcels_orig (x_coord, y_coord);')
             
    def split_places(self):
        "Break the all parcels file into seperate files by city, neighborhood, etc. "
        _, places = self.library.dep('places')
        all_parcels = self.partitions.find(table='parcels', space='all')
        lr = self.init_log_rate(1000)
        for place in places.query("SELECT * FROM places"):
            split_parcels = self.partitions.find_or_new_geo(table='parcels', space=place['code'])
            
            with split_parcels.database.inserter() as ins:
                for parcel in all_parcels.query(
                        "SELECT *, AsText(geometry) as wkt FROM parcels WHERE {} = ?".format(place['type']), 
                        place['code']):
                  
                    lr('Split parcels {} {}'.format(place['type'], place['code']))
                    ins.insert(parcel)
            
   
import sys

if __name__ == '__main__':
    import databundles.run

    databundles.run.run(sys.argv[1:], Bundle)    
    