import pyspark
import re

sc = pyspark.SparkContext()
vinFile = sc.textFile("/data/bitcoin/vin.csv")
voutFile = sc.textFile("/data/bitcoin/vout.csv")

def voutCorrect(line):
    fields = line.split(',')
    if len(fields)!=4:
        return False
    return True

def vinCorrect(line):
    fields = line.split(',')
    if len(fields)!=3:
        return False
    return True

voutWikiLeaks = voutFile.filter(lambda transact: transact.split(',')[3]=="{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}").map(lambda transact: transact.split(','))
voutWikiLeaksCorrect = voutWikiLeaks.filter(voutCorrect).map(lambda transact: (transact[0], transact[1]))

vinAllSplit = vinFile.map(lambda vinTransact: vinTransact.split(','))
vinAllCorrect = vinAllSplit.filter(vinCorrect).map(lambda vinTransact:(vinTransact[0],(vinTransact[1],vinTransact[2])))

vinWikiLeaks = vinAllCorrect.join(voutWikiLeaksCorrect)
vinWikiLeaksCorrect = vinWikiLeaks.map(lambda joinedTransact: (joinedTransact[1][0], joinedTransact[1][1]))

voutAllCorrect = voutFile.map(lambda vout: vout.split(',')).filter(voutCorrect).map(lambda vout: ((vout[0], vout[2]), (vout[3], vout[1])))

walletValue = voutAllCorrect.join(vinWikiLeaksCorrect).map(lambda transact: (transact[1][0][0], float(transact[1][0][1])))

walletValueSummed = walletValue.reduceByKey(lambda value1,value2: value1+value2)

donor10 = final_final.takeOrdered(10, key = lambda x: -x[1])

for i in range(10):
    print("Wallet: {} - Bitcoin: {} ".format(donor10[i][0],donor10[i][1])))
