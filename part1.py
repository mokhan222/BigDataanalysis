from mrjob.job import MRJob
import time
class Part1(MRJob):
    def mapper(self, _, line):
        try:
            fields = line.split(',')
            #extract date from index 2.
            date = int(fields[2])
            #convert from epoch to string date
            transactionDate = time.strftime("%m/%Y",time.gmtime(date))
            #emit date with count 1
            yield(transactionDate, 1)
        except:
            pass
    def combiner(self, key, values):
        #sum up all values in mapper for that key
        yield(key, sum(values))

    def reducer(self, key, values):
        #sum up all values for certain key.
        yield(key, sum(values))
if __name__ == '__main__':
    Part1.run()
