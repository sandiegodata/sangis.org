'''

'''

from  databundles.bundle import BuildBundle
from databundles.util import AttrDict

class Bundle(BuildBundle):
    ''' '''
 
    def __init__(self,directory=None):
        self.super_ = super(Bundle, self)
        self.super_.__init__(directory)

    def meta(self):
        self.googleize_zips()
        return True
        
    def googleize_zips(self):
        """Create a mapping between zip codes and city, to fix city errors in the addresses database. """
        from time import sleep
        import requests
        from pprint import pprint
        import yaml
        
        url = "http://maps.google.com/maps/api/geocode/json?address={zip}+CA&components=city&sensor=false"
        
        _, addresses = self.library.dep('addresses')
        addresses = self.partitions.find_or_new(table='addresses')

        zips = {}
        
        for row in addresses.query("SELECT DISTINCT zip FROM addresses"):

            if not row[0]:
                continue
            
            zip  = int(row[0])
        
            r  = requests.get(url.format(zip=row[0]))

            sn = None
            for c in r.json()['results'][0]['address_components']:
                if 'locality' in c['types'] or 'sublocality' in c['types']:
                    print zip,'-->',c['short_name']
                    sn = c['short_name']
                    zips[zip] = str(sn)
                    
            if not sn:
                pprint(r.json()['results'])
                    
            sleep(.3)
            
        with open(self.filesystem.path('meta','zips.yaml'), 'w') as f:
            yaml.dump(zips, f, indent=4, default_flow_style=False)
            
  


    def build(self):
        from databundles.identity import PartitionIdentity
        
        self.load_codes()
 
        self.partitions.new_geo_partition( PartitionIdentity(self.identity, table='addresses'), 
                                           shape_file = self.config.build.sources.addresses)
                                           
        self.partitions.new_geo_partition( PartitionIdentity(self.identity, table='roads'),
                                           shape_file = self.config.build.sources.roads)
                                           
        self.partitions.new_geo_partition( PartitionIdentity(self.identity, table='intersections'),
                                            shape_file = self.config.build.sources.intersections)
                
        self.add_indexes()
                
        return True

    def load_codes(self):
        import json
        
        codes = self.filesystem.load_yaml(self.config.build.codes)
        
        self.database.query("DELETE FROM codes")
        
        #
        # Load the codes copied from the metadata ( The PDF file for the dataset )
        # 
        
        with self.database.inserter("codes") as ins:
            for group, s in codes.codes.items():
                for k, v in s.items():
                    ins.insert({
                      'group': group,
                      'key': k,
                      'value': v
                    })
               
        #
        # Load in zip code mappings for cities
        # 
                
        name_map = {'La Jolla' : 'San Diego'}
                    
        zips = self.filesystem.load_yaml(self.config.build.zips)
            
        with self.database.inserter("codes") as ins:
            for zip, city in zips.items():

                ins.insert({
                  'group': 'zips',
                  'key': str(zip),
                  'value': name_map.get(city, city)
                })

        cities = {row['key']:row['value'] for row in 
                  self.database.query("SELECT * FROM codes WHERE `group` = 'jurisdiction' ")}

        incorp_cities = set(cities.values())

        zips =  {int(row['key']):row['value'] for row in 
                  self.database.query("SELECT * FROM codes WHERE `group` = 'zips' ")}

        all_cities = set(zips.values())

        with self.database.inserter("codes") as ins:
            for c in all_cities:
                ins.insert({
                  'group': 'places',
                  'key': c,
                  'value': 1 if c in incorp_cities else 0
                })

    def add_indexes(self):
        
        p = self.partitions.find(table='roads')
        p.database.query('CREATE INDEX IF NOT EXISTS road_rd20name_idx ON roads (rd20name);')
        p.database.query('CREATE INDEX IF NOT EXISTS road_block_idx ON roads (l_block, r_block);')
        p.database.query('CREATE INDEX IF NOT EXISTS road_nodes_idx ON roads (fnode, tnode);')
        
        p = self.partitions.find(table='addresses')
        p.database.query('CREATE INDEX IF NOT EXISTS adr_name_idx ON addresses (addrname);')       
        p.database.query('CREATE INDEX IF NOT EXISTS adr_number_idx ON addresses (addrnmbr);')   
        p.database.query('CREATE INDEX IF NOT EXISTS adr_coord_idx ON addresses (x_coord, y_coord);')       

        p = self.partitions.find(table='intersections')
        p.database.query('CREATE INDEX IF NOT EXISTS intr_interid_idx ON intersections (interid);')       

                
    def test_geo(self):
        
        _, blocks = self.library.dep('blocks')
        
        for row in p.query("select AsText(geometry) from addresses limit 5"):
            print row

import sys

if __name__ == '__main__':
    import databundles.run
      
    databundles.run.run(sys.argv[1:], Bundle)
     
    
    