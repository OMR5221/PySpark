# Search for errors in log files:

# Reason why we have the getOrCreate code
# http://stackoverflow.com/questions/28999332/how-to-access-sparkcontext-in-pyspark-script
sc = SparkContext.getOrCreate()


#load log files from local file system:
# get any files in vaar/log with filename like hadoop-hdfs-*
logfilesrdd = sc.textFile("file:///var/log/hadoop/hdfs/hadoop-hdfs-*")

# filter log records for errors:
# For each line check for "ERROR" in the line str
# Keep on those instances found
onlyerrorsrdd = logfilesrdd.filter(lambda line: "ERROR" in line)

# save errors to separate file:
onlyerrorsrdd.saveAsTextFile("file:///tmp/onlyerrorsrdd")