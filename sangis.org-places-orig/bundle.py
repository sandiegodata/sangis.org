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
                Area(geo) as areax
                FROM ( SELECT *, CastToMultiPolygon(GUnion(Buffer(geometry,0.0))) as geo FROM {} GROUP BY {})
                """.format(table, group_name))):
                    drow = dict(row)


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


    def update_places(self):

        temp = self.partitions.find(grain='temp', tables=('places'))

        # Buffer rebuilds the geometry to get rid of non-noded intersections. 
        for row in temp.query("SELECT name, code, GUnion(Buffer(geometry,0.0)) FROM places GROUP BY city, code"):
            print row

    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    