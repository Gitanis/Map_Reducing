from mrjob.job import MRJob
from mrjob.step import MRStep

#importing datetime library to handle timestamp data
from datetime import datetime
# We extend the MRJob class
# This includes our definition of map and reduce functions
class AvgRevenueByDay(MRJob):

    def mapper(self, _, line):
    
        days = {1:'Monday',2:'Tuesday',3:'Wednesday',4:'Thursday',5:'Friday',6:'Saturday',7:'Sunday'}
        
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID':
                
                pickup_time = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")  
                              
                weekday = pickup_time.isoweekday()  
                
                dayofweek = days.get(weekday)   

                amount = float(row[16])         
                
                yield dayofweek, amount
            
    def reducer(self, day, amount):       
        count = 0
        total = 0
        average_revenue = 0

        for amt in amount:
            count+= 1
            total+= amt
  
        average_revenue = total/count
        
        
        yield day, average_revenue      

if __name__ == '__main__':
    AvgRevenueByDay.run()