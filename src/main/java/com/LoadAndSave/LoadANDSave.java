package com.LoadAndSave;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * load加载文件中数据，将数据写入文件中
 */
public class LoadANDSave {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("LoadANDSave");
        conf.set("spark.driver.memory","512m").set("spark.testing.memory","1073740000");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        //read后面可以指定加载什么样式文件，load：加载所有支持的文件类型
        DataFrame userDF = sqlContext.read().load("D:\\ideaProject2\\spark-job\\000000_0");

        //选择查询指定数据写入文件里
        userDF.select("name","age").write().save("D:\\ideaProject2\\spark-job\\teacher.parquet");
        /*userDF.printSchema();
        userDF.show();*/


    }
}
