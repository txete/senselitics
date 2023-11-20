
import json

def extraerDatos(spark,schema,pipeline):
    pipeline_json = json.dumps(pipeline)
    return spark.read.format("mongo").option("pipeline", pipeline_json).schema(schema).option("partitioner", "MongoSinglePartitioner").load().repartition(1)
