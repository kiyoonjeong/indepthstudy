from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def main():
    # Task configuration.
    topic = "heartbeat"
    brokerAddresses = ["b-2.demo-cluster-1.mx96ok.c2.kafka.us-east-1.amazonaws.com:9094","b-1.demo-cluster-1.mx96ok.c2.kafka.us-east-1.amazonaws.com:9094"]
    batchTime = 20
    
    # Creating stream.
    spark = SparkSession.builder.appName("PythonHeartbeatStreaming").getOrCreate()
    sc = spark.sparkContext
    ssc = StreamingContext(sc, batchTime)    
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokerAddresses})

    # The streaming can be handled as an usual RDD object.
    # for example: kvs.map(lambda l : l.lower())
    kvs.pprint() # Just printing result on stdout.
    
    # Starting the task run.
    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()