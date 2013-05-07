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

        return True


    def copy_currentuse(self):
        import os 
        
        cuo = self.partitions.find(table='currentuse_orig')
        
        cu = self.partitions.find_or_new_geo(table='currentuse')


        srs = cuo.get_srs()

        lr = self.init_log_rate(N=2000, message="Copy orig")

        try: cu.query("DELETE FROM  currentuse")
        except: pass
        
        # Using ExteriorRing because there is an error in an internal ring in one of the parcels. 
        with cu.database.inserter(source_srs=srs, dest_srs=srs) as ins:
            for row in cuo.query("""
            SELECT lu as code, landuse as name, AsText(geometry) AS wkt, Area(geometry) AS area
            FROM currentuse_orig
            """):

                lr()
           
                ins.insert(dict(row))
                
                
                

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)  
    