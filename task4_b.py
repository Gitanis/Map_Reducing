from mrjob.job import MRJob
from mrjob.step import MRStep

# We extend the MRJob class
# This includes our definition of map and reduce functions
class PickupLocMaxRevenue(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_1),
            MRStep(reducer=self.reducer_2) ]

    def mapper(self, _, line):
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID':        
                pickuploc  = row[7]             
                amount = float(row[16])     

                yield pickuploc, amount         

                
    def reducer_1(self, pickuploc, amount):     
        
        yield None, (sum(amount),pickuploc)     

    
    def reducer_2(self, _, amount_pickuploc_pair):         
        result = max(amount_pickuploc_pair)
        
        yield result[1], result[0]              


if __name__ == '__main__':
    PickupLocMaxRevenue.run()
        
        