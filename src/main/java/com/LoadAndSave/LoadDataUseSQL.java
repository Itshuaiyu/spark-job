package com.LoadAndSave;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 *spark session使用
 * load方式读取数据后进行spark-sql处理
 */
public class LoadDataUseSQL {

    public static void main(String[] args) {
        //构建spark参数
        //todo 使用sparkSession
        /*SparkSession sparkSession = SparkSession.builder().appName("LoadDataUseSQL").master("local").getOrCreate();
        SQLContext sqlContext = new SQLContext(sparkSession);*/
        SparkConf conf = new SparkConf().setMaster("local").setAppName("LoadDataUseSQL");
        //创建java-spark连接
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        //创建spark-sql连接
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        //读取加载指定位置的文件
        sqlContext.read().load("D:\\ideaProject2\\spark-job\\teacher.parquet").registerTempTable("user");
        //创建临时表user
        DataFrame sql = sqlContext.sql("select * from user");

        JavaRDD<Row> rowJavaRDD = sql.toJavaRDD();

        //使用RDD：map类型转化
        List<String> collect = rowJavaRDD.map(new Function<Row, String>() {
            public String call(Row row) throws Exception {
                //todo row获取第一行的第一个字符
                return "name:" + row.getString(0);
            }
        }).collect();

        for (String loadSQLADD : collect
             ) {
            System.out.println(loadSQLADD);
        }
    }
}
