from mrjob.job import MRJob

from datetime import datetime

# We extend the MRJob class
# This includes our definition of map and reduce functions
class AverageTripTime(MRJob):

    def mapper(self, _, line):
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID':        
                pickup_loc = row[7]              
                pickup_time = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")  
                dropoff_time = datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S")  
                
                duration =(dropoff_time - pickup_time).total_seconds()      
            
                yield pickup_loc,duration      

                   
    def reducer(self, pickup_loc, duration):       
        time = 0
        count = 0
        total = 0
        average_dur = 0
        result = ''

        for time in duration:
            count+= 1
            total+= time
      
        average_dur = int(total/count)
        
        hours = average_dur // 3600
        minutes = (average_dur % 3600) // 60
        seconds = average_dur % 60

        result = str(hours) + 'hours ' + str(minutes) + 'minutes ' + str(seconds) + 'seconds'
        
        yield pickup_loc,result      

if __name__ == '__main__':
    AverageTripTime.run()
