package com.sparkHive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple10;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 从mysql表中读取数据
 */
public class MysqlDataSource {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("HiveDataSource");

        JavaSparkContext sc = new JavaSparkContext(conf);
        //创建hiveContext.注意这里接收的是sparkContext，不是JavaSparkContext
        SQLContext sqlContext = new SQLContext(sc);
        // 总结一下
        // jdbc数据源
        // 首先，是通过SQLContext的read系列方法，将mysql中的数据加载为DataFrame
        // 然后可以将DataFrame转换为RDD，使用Spark Core提供的各种算子进行操作
        // 最后可以将得到的数据结果，通过foreach()算子，写入mysql、hbase、redis等等db / cache中

        // 分别将mysql中两张表的数据加载为DataFrame
        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:mysql://localhost:3306/flink-sql");
        options.put("dbtable","t_instance_redis");
        DataFrame instanceDF = sqlContext.read().format("jdbc").options(options).load();

        options.put("dbtable","t_logmon_mysqlk8s");
        DataFrame logmonDF = sqlContext.read().format("jdbc").options(options).load();

        JavaPairRDD<String, Tuple2<String, String>> joinRDD = instanceDF.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<String, String>(row.getString(1), row.getString(2));
            }
        }).join(logmonDF.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<String, String>(row.getString(2), row.getString(4));
            }
        }));

        //将joinRDD转化为JAVARDD
        JavaRDD<Row> resultRDD = joinRDD.map(new Function<Tuple2<String, Tuple2<String, String>>, Row>() {

            @Override
            public Row call(Tuple2<String, Tuple2<String, String>> tuple) throws Exception {
                return RowFactory.create(tuple._1, tuple._2._1, tuple._2._2);
            }
        });

        //数据格式封装
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("ip", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("work_zone", DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("zone_code", DataTypes.StringType,true));
        StructType structType = DataTypes.createStructType(structFields);

        //todo 使用filter算子过滤ip=192.11.12.3的数据

        JavaRDD<Row> filterRDD = resultRDD.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                if (row.getString(1).equals("192.11.12.3")) {
                    return true;
                }
                return false;
            }
        });

        //转化为DataFrame
        DataFrame dataFrame = sqlContext.createDataFrame(filterRDD, structType);

        Row[] rows = dataFrame.collect();
        for (Row row : rows
                ) {
            System.out.println(row);
        }

        //todo 将DataFrame中的数据保存到mysql表中
        // 这种方式是在企业里很常用的，有可能是插入mysql、有可能是插入hbase，还有可能是插入redis缓存
        dataFrame.javaRDD()
    }
}
