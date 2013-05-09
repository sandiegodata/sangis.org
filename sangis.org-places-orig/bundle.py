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
        with open (self.filesystem.path('meta','source_schema.csv'), 'wb') as f:
            self.schema.as_csv(f)
            
        return True


    def build(self):
        from databundles.identity import PartitionIdentity

        for name, url in self.config.build.sources.items():
            table_name = name+'_orig'
            pid = PartitionIdentity(self.identity, table=table_name)
            
            p = self.partitions.new_geo_partition(pid, shape_file=url)


    
        self.build_places()

        self.schema.add_views()
        
        self.add_spcs()

        return True

    def build_places(self):

        col_map = self.filesystem.read_csv(self.filesystem.path('meta','colmap.csv'), ('table', 'outcol'))

        placep = self.partitions.find_or_new_geo(table='places')

        try: placep.query("DELETE FROM places")
        except: pass

            
        # The original files define the SRS directly, so lets make that an 
        # authoritative value
        source_srs = self.config.build.source_srs


        with placep.database.inserter('places') as ins:
            for p in self.partitions:
                
                if not p.identity.name.endswith('orig'):
                    continue
                
                table = str(p.identity.table)
                city = None
                
                mapping = col_map.get( (table,"name"), False )
                group_name = mapping['group']
                
                for i, row in enumerate(p.database.query("""
                SELECT *,
                AsText(Transform(geo, 4326)) AS wkt,
                X(Transform(Centroid(geo), 4326)) AS lon, 
                Y(Transform(Centroid(geo), 4326)) as lat,
                Area(geo) as areax,
                AsText(Envelope(geo)) as envelope
                FROM ( SELECT *, CastToMultiPolygon(GUnion(Buffer(geometry,0.0))) as geo FROM {} GROUP BY {})
                """.format(table, group_name))):
                    drow = dict(row)

                    drow['origsrs'] = source_srs

                    for oc in ('code','name'):
                        mapping = col_map.get( (table,str(oc)), False )
    
                        ic = mapping['incol']
                        
                        if '{' in ic:
                            v = ic.format(i=i,**drow)
                        else:
                            v = drow[ic]
                        
                        drow[oc] = v
    
                    drow['area'] = drow['areax']
                     
                    drow['name'] = drow['name'].title()
                    
                    drow['type'] = table.replace('_orig','')
                    
                    if '_' in drow['type']:
                        city, type_ = drow['type'].split('_',2)
                    else:
                        city = None
                        type_ = drow['type']
                        
                    drow['type'] = type_
                    drow['city'] = city    

                        
                    ins.insert(drow)

  
    def add_spcs(self):
        """Attach the SPCS zones ( Which will always be California 6 ) and the envelopes """
        import ogr, osr
        import json
        
        from databundles.geo.util import segment_points
        from databundles.geo.analysisarea import AnalysisArea
        
        temp = self.partitions.find(grain='temp', tables=('places'))

        _, zones = self.library.dep('spzones')

        places = self.partitions.find(table='places')
        placessrs = places.get_srs()

      # Spatiaite takes over the primary key in geo partitions. 
        places.query("UPDATE places SET places_id = OGC_FID")



        for zone, where, is_in in segment_points(zones, 
                                        "SELECT *, AsText(geometry) AS wkt FROM spzones",
                                        "lon BETWEEN {x1} AND {x2} AND lat BETWEEN {y1} and {y2}"):

            with places.database.updater() as upd:
                for place in places.query("SELECT *, AsText(geometry) AS wkt  FROM places WHERE {}".format(where)):
    
                    if is_in(place['lon'], place['lat']):
                        self.log("Attach {} to {}".format(place['name'], zone['name']))
                    
                        spsrs =  ogr.osr.SpatialReference()
                        spsrs.ImportFromEPSG(zone['srid'])
                        
                        transform = osr.CoordinateTransformation(placessrs,spsrs)   
                        g = ogr.CreateGeometryFromWkt(place['wkt'])
                        wgsenvelope = g.GetEnvelope()
                        g.Transform(transform)
                        utmenvelope = g.GetEnvelope()

                        aa = AnalysisArea.new_from_envelope(spsrs, utmenvelope, scale=1)
                        aa.name = place['name']
                        aa.geoid = place['code']
                      
                        
                        u = {
                             '_OGC_FID': place['OGC_FID'],
                             '_spsrs': zone['srid'],
                             '_wgsenvelope' : json.dumps(aa.ll_envelope),
                             '_utmenvelope' : json.dumps(aa.ne_envelope),
                             '_aa' : aa.to_json()
                            }
          
                        upd.update(u)
    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    