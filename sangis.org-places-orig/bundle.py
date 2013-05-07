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
            pid = PartitionIdentity(self.identity, table=name+'_orig')
            p = self.partitions.new_geo_partition(pid, shape_file=url)

    
        self.build_places()

        self.schema.add_views()

        return True

    def build_places(self):

        col_map = self.filesystem.read_csv(self.filesystem.path('meta','colmap.csv'), ('table', 'outcol'))

        placep = self.partitions.find_or_new_geo(table='places')

        try: placep.query("DELETE FROM places")
        except: pass

        with placep.database.inserter() as ins:
            for p in self.partitions:
                
                if not p.identity.name.endswith('orig'):
                    continue
                
                table = str(p.identity.table)
                city = None
                
                for i, row in enumerate(p.database.query("""
                SELECT *, 
                X(Centroid(geometry)) AS x, 
                Y(Centroid(geometry)) as y,
                X(Transform(Centroid(geometry), 4326)) AS lon, 
                Y(Transform(Centroid(geometry), 4326)) as lat,
                AsText(Transform(CastToMultiPolygon(geometry), 4326)) AS wkt 
                FROM {}
                """.format(table))):
                    drow = dict(row)
                      
                    for oc in ('code','name'):
                        mapping = col_map.get( (table,str(oc)), False )
    
                        ic = mapping['incol']
                        
                        if '{' in ic:
                            v = ic.format(i=i,**drow)
                        else:
                            v = drow[ic]
                        
                        drow[oc] = v
    
                     
                    drow['name'] = drow['name'].title()
                    
                    drow['type'] = table.replace('_orig','')
                    
                    if '_' in drow['type']:
                        city, type_ = drow['type'].split('_',2)
                    else:
                        city = None
                        type_ = drow['type']
                        
                    drow['type'] = type_
                    drow['city'] = city    
                        
                    #if type != 'city':
                    #    print row['wkt']
                        
                    ins.insert(drow)

    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    