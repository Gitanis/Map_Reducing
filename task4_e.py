from mrjob.job import MRJob
from mrjob.step import MRStep

# We extend the MRJob class
# This includes our definition of map and reduce functions
class TotalAmtTipRatio(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                  reducer=self.reducer_1),
            MRStep(reducer=self.reducer_2) ]

    def mapper(self, _, line):
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID':        
                pickup_loc  = row[7]             
                amount = float(row[16])     
                tip    = float(row[13])     
                
                yield pickup_loc, (amount,tip)

    def reducer_1(self, pickup_loc, amount_pair):
        total_amount = 0
        total_tip = 0

        for amt in amount_pair:
            total_amount = total_amount + amt[0]
            total_tip  = total_tip + amt[1]
        
        if total_amount == 0:
            average_ratio = 0
        else:
            average_ratio = total_tip/total_amount
             
        yield None, (average_ratio,pickup_loc)
    
    def reducer_2(self, _, result_pair):  
        sorted_result = sorted(result_pair, reverse = True)
        for pair in sorted_result:
            yield pair[1], pair[0]

if __name__ == '__main__':
    TotalAmtTipRatio.run()            