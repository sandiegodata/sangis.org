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

        source_srs = self.config.build.source_srs

        for name, url in self.config.build.sources.items():
            pid = PartitionIdentity(self.identity, table=name)
            p = self.partitions.new_geo_partition(pid, shape_file=url)

        return True



    
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    