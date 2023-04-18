package com.autoPartition;

import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class ParquetPartition {
    public static void main(String[] args) {
      /*  SparkSession sparkSession = SparkSession.builder().appName("LoadDataUseSQL").master("local").getOrCreate();
        SQLContext sqlContext = new SQLContext(sparkSession);
        Dataset<Row> parquet = sqlContext.read().parquet("hdfs://user/hive/warehouse/user/gender=male/country=us/teacher.parquet");
        parquet.show();
        parquet.printSchema();*/
    }
}
