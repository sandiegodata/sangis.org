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

    def get_zone(self):
        """Return the State Plane zone, and the SRS, for San Diego. """
        
        _, places = self.library.dep('places')
        
        
        sd = places.query("SELECT * FROM places where code = 'SD' LIMIT 1").first()
        
        return sd['spsrs']
  
    def split_docs(self):
        """Split the original parcel records into parcels, with a few extra fields, and parcel docs, 
        with just the situs and assessors information"""
        
        parcels = self.partitions.find_or_new_geo(table='parcels', space='all')
        parceldocs = self.partitions.find_or_new(table='parceldocs', space='all')
        parcels_o = self.partitions.find(table='parcels_orig')
        
        lr = self.init_log_rate(2000)
        
        p_ins =  parcels.database.inserter()
        pd_ins =  parceldocs.database.inserter()
             
        # Get the stateplane SRS number
        spsrs = self.get_zone()
                
        for row in parcels_o.query(""" SELECT *, AsText(Transform(geometry, {spsrs})) as wkt FROM parcels_orig""".format(spsrs=spsrs)):
        
            row = dict(row)
        
            lr("Copy parcels")
            
            p_ins.insert(row)
            pd_ins.insert(row)            
        
        
        # Do this  seperately because they all depend on the geomoetry being tansformed in the previous query
        parcels.query("""
        UPDATE parcels SET 
            area = Area(geometry),
            x = X(Centroid(geometry)), y = Y(Centroid(geometry)),
            lon = X(Transform(Centroid(geometry), 4326)), lat = Y(Transform(Centroid(geometry), 4326));
        """)
    

    def add_places(self):      
        from databundles.geo.util import segment_points
        
        parcels = self.partitions.find_or_new(table='parcels', space='all')
        
        # Spatialite takes over the primary key
        #parcels.query("UPDATE parcels SET parcels_id = objectid");
        #parcels.query('CREATE INDEX IF NOT EXISTS parcels_id_idx ON parcels (parcels_id);')
        
        lr = self.init_log_rate(1000)
        
        _, places = self.library.dep('places')
        
        for area, where, is_in in segment_points(places, 
                                                 "SELECT *, AsText(geometry) AS wkt FROM places ORDER BY area ASC"):

            count = 0;
            self.log("Area {}".format(area['name']))
            with parcels.database.updater() as upd:
                for parcel in parcels.query("SELECT *, AsText(geometry) as wkt FROM parcels  WHERE {} AND {} IS NULL ".format(where, area['type'])):
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
            
            
    def group_parcels(self):
        import osr, ogr
            
        groups = set()
        
        all_parcels = self.partitions.find(table='parcels', space='all')
        where_template = ""
        for p1 in all_parcels.query("""SELECT *, AsText(geometry) AS wkt  FROM parcels WHERE asr_zone = 6 ORDER BY area"""):
            p1g = ogr.CreateGeometryFromWkt(p1['wkt'])
            p1b = p1g.Buffer(1000)
            e = p1b.GetEnvelope()
        
            count = 0
            
            q = """
                SELECT * FROM parcels 
                WHERE asr_zone = 6 AND OGC_FID != {id}
                AND x BETWEEN {x1} AND {x2} AND y BETWEEN {y1} and {y2}
                ORDER BY area""".format(id=p1['OGC_FID'], x1=e[0], x2=e[1], y1=e[2], y2=e[3])
            
            self.log(q)
            
            for p2 in all_parcels.query(q):
                
                count += 1
                p2g = ogr.CreateGeometryFromWkt(p1['wkt'])
                
                if p1g.Distance(p2g) < 300:
                    id1  = p1g.OGC_FID
                    id2  = p2g.OGC_FID
                    
                    if id1 > id2:
                        id1, id2 = id2, id1
                    
                    groups.add( (id1, id2) )
                    
            self.log("Checked {} Groups: {}".format(count, len(groups)))
                    
        
import sys

if __name__ == '__main__':
    import databundles.run

    databundles.run.run(sys.argv[1:], Bundle)    
    