from mrjob.job import MRJob
from mrjob.step import MRStep

#importing datetime library to handle timestamp data
from datetime import datetime

class AvgRevenueByHour(MRJob):
    
    def mapper(self, _, line):
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID': 
                
                pickup_time = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")            
                hour = pickup_time.hour             
                hour_day = 'Hour ' + str(hour)  
                amount = float(row[16])        

                
                yield hour_day, amount         
                
                
    def reducer(self, hour_day, amount):       
        count = 0
        total = 0
        average_revenue = 0

        for amt in amount:
            count+= 1
            total+= amt
                 
        average_revenue = total/count
        
       
        yield hour_day, average_revenue      

if __name__ == '__main__':
    AvgRevenueByHour.run()              
                
                