import os
import pandas as pd
from hdfs import InsecureClient


# HDFS commection
client_hdfs = InsecureClient('http://sandbox-hdp.hortonworks.com:50070')


def serialize(name):
    hdfs_path = '/user/root/data/' + str(name)
    hdfs_path_serialized = '/user/root/parquet/' + name.replace('.csv', '.parquet')
    local_path = '/root/hdfs_homework/' + str(name).replace('.csv', '.parquet')
    with client_hdfs.read(hdfs_path, encoding = 'utf-8') as reader:
         df = pd.read_csv(reader, nrows = 10000)
    df.to_parquet(name.replace('.csv', '.parquet'))
    client_hdfs.upload(hdfs_path_serialized, local_path)
    os.remove(local_path)


serialize('destinations.csv')
serialize('sample_submission.csv')
serialize('test.csv')
serialize('train.csv')
