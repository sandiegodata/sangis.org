'''
'''

from  ambry.bundle.loader import GeoBuildBundle

class Bundle(GeoBuildBundle):
    ''' '''

    def build(self):
        from ambry.identity import PartitionNameQuery
        
        super(Bundle, self ).build()

        p = self.partitions.find(PartitionNameQuery(table='parcels'))
        
        pdq = p.database.query
        
        #pdq('CREATE INDEX IF NOT EXISTS apn_idx ON parcels (apn, apn_8);')
        #pdq('CREATE INDEX IF NOT EXISTS parcel_idx ON parcels (parcelid);')
        #pdq('CREATE INDEX IF NOT EXISTS road_idx ON parcels (addrno, roadname);')
        #pdq('CREATE INDEX IF NOT EXISTS sroad_idx ON parcels (situs_addr, situs_stre);')
        #pdq('CREATE INDEX IF NOT EXISTS zip_idx ON parcels (zip);')
        #pdq('CREATE INDEX IF NOT EXISTS szip_idx ON parcels (situs_zip);')
        #pdq('CREATE INDEX IF NOT EXISTS jur_idx ON parcels (situs_juri);')
        #pdq('CREATE INDEX IF NOT EXISTS coord_idx ON parcels (x_coord, y_coord);')

        return True
        
    def containment(self):
        
        
        from ambry.geo.util import  find_containment

        lr = self.init_log_rate(3000)

        def gen_bound():
        
            places = self.library.dep('places').partition

            lr = self.init_log_rate(3000)

            # Note, ogc_fid is the primary key. The id column is created by the shapefile. 
            for i,boundary in enumerate(places.query(
                "SELECT  AsText(geometry) AS wkt, code, scode, name FROM places WHERE type = 'community' ")):
                
                lr('Load rtree')
 
                yield i, boundary['wkt'] , ( boundary['scode'], boundary['code'] ,  boundary['name']  )
    
        def gen_points():

            parcels = self.library.dep('parcels').partition
        
            lr = self.init_log_rate(3000)
        
            for i,parcel in enumerate(parcels.query(
                """SELECT  X(Transform(Centroid(geometry), 4326)) AS lon, 
                Y(Transform(Centroid(geometry), 4326)) AS lat, apn FROM parcels 
                WHERE situs_juri = 'SD' """)):
                
                lr('Generate points')
 
                yield (parcel['lon'], parcel['lat']), (parcel['apn'], parcel['lat'], parcel['lon'])
                
        import csv
        lr = self.init_log_rate(300)
        with open('parcel_communities.csv', 'w') as f:
            w = csv.writer(f)
            w.writerow(['apn', 'lat','lon', 'source_code', 'community_code','community_name'])
            
            for point, point_o, cntr_geo, cntr_o in find_containment(gen_bound(),gen_points()):
                lr('Write row')
                
                w.writerow([point_o[0], point_o[1], point_o[2] ,cntr_o[0], cntr_o[1], cntr_o[2]])
   
                
   
            
    
