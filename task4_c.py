
from mrjob.job import MRJob
from mrjob.step import MRStep

class PaymentTypeCounts(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,                   
                  reducer=self.reducer_1),
            MRStep(reducer=self.reducer_2) ]
           
    def mapper(self, _, line):                      
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID':        
                paymenttype  = row[9]             
                count  = 1                   
            
                yield paymenttype,count                           
        
    
    def reducer_1(self, paymenttype, count):       
        
         yield None, (sum(count),paymenttype)
            

    def reducer_2(self, _, result_pair):  
       
        sorted_result = sorted(result_pair, reverse = True)
        for pair in sorted_result:
            yield pair[1], pair[0]            
                                   
            
if __name__ == '__main__':
    PaymentTypeCounts.run()                
