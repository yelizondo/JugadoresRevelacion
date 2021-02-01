from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (DateType, FloatType, StructField, StructType, StringType)
import json

spark = SparkSession.builder.appName("Calculate score").getOrCreate()

mapr_schema = StructType([StructField('name2', StringType()), StructField('market', FloatType())])
hive_schema = StructType([StructField('name', StringType()), StructField('team', StringType()), StructField('stats', FloatType())])

hive_dataframe = spark.read.csv("hive.csv", schema=hive_schema, header=False)

mapr_dataframe = spark.read.csv("mapr.csv", schema=mapr_schema, header=False)

shared = hive_dataframe.join(mapr_dataframe,hive_dataframe["name"]==mapr_dataframe["name2"],"inner")
shared = shared.drop("name2")

dictionary = {}

def depreciate(stats, res):
    #print("Depreciate")
    #print("Init stats: ", stats)
    if res <= 0.1:
        stats = stats * (1 - res)
    elif res > 0.1 and res <= 0.2:
        stats = stats * (1 - res*1.2)
    elif res > 0.2 and res <= 0.3:
        stats = stats * (1 - res*1.4)
    elif res > 0.3 and res <= 0.4:
        stats = stats * (1 - res*1.6)
    elif res > 0.4 and res <= 0.5:
        stats = stats * (1 - res*1.8)
    else:
        stats = stats * 0.5
    #print("End stats: ", stats)
    #print("")
    return stats


def inflate(stats, res):
    #print("Inflate")
    #print("Init stats: ", stats)
    if res >= 0.9:
        stats = stats + (stats/2) * res
    elif res > 0.1 and res <= 0.2:
        stats = stats + (stats/2) * (res * 0.8)
    elif res > 0.2 and res <= 0.3:
        stats = stats + (stats/2) * (res * 0.6)
    elif res > 0.3 and res <= 0.4:
        stats = stats + (stats/2) * (res * 0.4)
    elif res > 0.4 and res <= 0.5:
        stats = stats + (stats/2) * (res * 0.2)
    else:
        stats = stats + (stats/2) * (res * 0.1)
    #print("End stats: ", stats)
    #print("")
    return stats

def equal(stats, res):
    return stats

teams = []

for row in shared.rdd.collect():
    name = row.name
    team = row.team
    stats = row.stats
    market = row.market

    res = market - stats

    if res < 0:
        val = depreciate(stats, abs(res))
    elif res > 0:
        val = inflate(stats, res)
    else:
        val = equal(stats, res)
    
    try:
        temp = dictionary[team]
        #temp["children"] = temp["children"] + [[name, val]]
        temp["children"] = temp["children"] + [{"name": name, "value": val}]
        dictionary[team] = temp
    except:
        mini1 = {"name": name, "value": val}
        macro = {"name": team, "children": [mini1]}
        dictionary[team] = macro
        teams = teams + [team]
    


dict2 = {}

for i in teams:
    f = dictionary[i]
    dict2.update(f)

json_object = json.dumps(dict2, indent = 4)   
print(json_object)  
    

#shared.show()
