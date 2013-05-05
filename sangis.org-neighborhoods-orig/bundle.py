'''
'''

from  databundles.bundle import BuildBundle
 

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)
 
    def build(self):
        from databundles.identity import PartitionIdentity

        pid = PartitionIdentity(self.identity, table='neighborhoods', space='sd')
        gp = self.partitions.new_geo_partition(pid, shape_file=self.config.build.sources.sdneighborhoods)

        pid = PartitionIdentity(self.identity, table='communities', space='sd')
        gp = self.partitions.new_geo_partition(pid, shape_file=self.config.build.sources.sdcommunities)

        self.add_views()

        return True

    def add_views(self):
        
        for p in self.partitions:

            if not p.table:
                continue
  
            views = self.config.views.get(p.table.name, False)
 
            if not views:
                continue
            
            for name, view in views.items():
                self.log("Adding view: {} to {}".format(name, p.identity.name))
                sql = "DROP VIEW IF EXISTS {}; ".format(name)
                p.database.connection.execute(sql)
                  
                sql = "CREATE VIEW {} AS {};".format(name, view)
                p.database.connection.execute(sql)           
            

     
import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)  
    