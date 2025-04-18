This is a readme file.

Running this repository - given your installations for kafka and spark are done

Step 0 : Python Prequisites
``` 
pip install pandas psycopg2-binary kafka-python pyspark
```

Step 1 :
In separate terminals:
```
sudo systemctl start zookeeper
sudo systemctl start kafka
sudo systemctl start postgresql
```

Step 2:
Batch Processor:
```
cd Batch_Stream_Comparison/batch_processing
python3 batch_query.py
```

Step 3 : Kafka producer:
```
cd Batch_Stream_Comparison/stream_processing
python3 producer.py
```

Step 4 : Stream Processor
```
spark-submit   --packages     org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.postgresql:postgresql:42.7.5 stream_query.py
```
