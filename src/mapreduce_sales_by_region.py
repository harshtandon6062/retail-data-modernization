"""MapReduce, the Hadoop way of thinking, written with Spark's low-level RDD API.

Hadoop MapReduce job = MAP -> SHUFFLE/SORT -> REDUCE:
  map    : each input line -> (key, value) pairs      here: (region, net_amount)
  shuffle: all pairs with the same key go to the same reducer (network + disk)
  reduce : combine the values of one key               here: sum
Hadoop writes to disk between every stage; Spark keeps data in memory, which is
why the same job is usually much faster in Spark. reduceByKey also pre-sums on
each partition before the shuffle (Hadoop calls that a "combiner").
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, banner, get_spark  # noqa: E402

if __name__ == "__main__":
    spark = get_spark("mapreduce_demo", hive=False)
    sc = spark.sparkContext
    banner("MapReduce-style job: net sales per region (RDD map + reduceByKey)")

    # Input: the exported fact + store files (plain text lines, like files in HDFS)
    stores = sc.textFile(str(ROOT / "exports" / "powerbi" / "dim_store.csv"))
    header = stores.first()
    store_region = dict(stores.filter(lambda line: line != header)
                              .map(lambda line: line.split(","))
                              .map(lambda f: (f[0], f[3])).collect())     # store_key -> region
    region_of = sc.broadcast(store_region)   # ship the small lookup to every worker once

    facts = sc.textFile(str(ROOT / "exports" / "powerbi" / "fact_sales.csv"))
    fheader = facts.first()
    cols = fheader.split(",")
    i_store, i_status, i_net = cols.index("store_key"), cols.index("order_status"), cols.index("net_amount")

    result = (facts.filter(lambda line: line != fheader)
                   .map(lambda line: line.split(","))
                   .filter(lambda f: f[i_status] not in ("CANCELLED", "RETURNED"))
                   # MAP: emit (key, value)
                   .map(lambda f: (region_of.value[f[i_store]], float(f[i_net])))
                   # SHUFFLE + REDUCE: sum values per key
                   .reduceByKey(lambda a, b: a + b)
                   .sortBy(lambda kv: -kv[1]))

    print(f"  input partitions: {facts.getNumPartitions()}  (Hadoop would start one map task per HDFS block)")
    for region, total in result.collect():
        print(f"  {region:<6} INR {total:>16,.2f}")
    print("  Lineage of the RDD (Spark's DAG of the job):")
    print("   ", result.toDebugString().decode().replace("\n", "\n    "))
    spark.stop()
