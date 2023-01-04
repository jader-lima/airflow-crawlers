import argparse
from os.path import join

from pyspark.sql import SparkSession

def write(df, dest, dest_format, mode):
    df.write.format(dest_format).mode(mode).save(dest)


def ingest_data(spark, src, dest, table_name, src_format, dest_format,  process_date):
    print(src)
    print(dest)
    print(table_name)
    print(src_format)
    print(dest_format)
    print(process_date)
    options_dict = {
        'sep': ';',
        'header': 'True',
        'encoding': 'ISO-8859-1',
    }
    df = spark.read.format(src_format).options(**options_dict).load(src)
    df.show()
    table_dest = join(dest, f"table_name={table_name}", f"process_date={process_date}")
    write(df, table_dest, dest_format, 'overwrite')



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ingestion"
    )
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--table_name", required=True)
    parser.add_argument("--src_format", required=True)
    parser.add_argument("--dest_format", required=True)
    parser.add_argument("--process_date", required=True)
    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName("ingestion")\
        .getOrCreate()

    ingest_data(spark, args.src, args.dest, args.table_name ,args.src_format, args.dest_format , args.process_date)




