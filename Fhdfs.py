import pyarrow as pa
import pyarrow.parquet as pq
from hdfs import InsecureClient
from io import BytesIO

def hdfsCliente():    
    return InsecureClient('http://localhost:50070', user='raj_ops', timeout=120)

def hdfsSubir(pandas_df):
    table = pa.Table.from_pandas(pandas_df)
    buffer = BytesIO()
    pq.write_table(table, buffer)
    client = hdfsCliente()
    with client.write('/user/raj_ops/report.parquet', overwrite=True) as writer:
        writer.write(buffer.getvalue())

def main():
    return True