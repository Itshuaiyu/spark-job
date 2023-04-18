package com.sparkHive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * 将本地数据加载到hive表中
 * 通过sql join两张表做查询，结果存到新表中
 * hive数据源
 */
public class HiveDataSource {
    public static void main(String[] args) {
        //首先创建SparkSession
//        SparkSession sparkSession = SparkSession.builder().appName("LoadDataUseSQL").master("local").getOrCreate();
//        SQLContext sqlContext = new SQLContext(sparkSession);

        SparkConf conf = new SparkConf()
                .setAppName("HiveDataSource");

        JavaSparkContext sc = new JavaSparkContext(conf);
        //创建hiveContext.注意这里接收的是sparkContext，不是JavaSparkContext
        HiveContext hiveContext = new HiveContext(sc.sc());

        hiveContext.sql("DROP TABLE IF EXISTS student_info");
        hiveContext.sql("CREATE TABLE IF EXISTS student_info(name STRING, age INT)");
        //讲学生数据导入student_info表
        hiveContext.sql("LOAD DATA" +
                "LOCAL INPATH '/root/test/student_info.txt'" +
                "INTO TABLE student_info");

        hiveContext.sql("DROP TABLE IF EXISTS student_scores");
        hiveContext.sql("CREATE TABLE IF EXISTS student_scores(name STRING, score INT)");
        //讲学生数据导入student_info表
        hiveContext.sql("LOAD DATA" +
                "LOCAL INPATH '/root/test/student_scores.txt'" +
                "INTO TABLE student_scores");

        //执行sql查询关联两张表，查询大于80分的学生
        DataFrame result = hiveContext.sql("select" +
                "from student_info s1" +
                "join student_score ss on s1.name = ss.name" +
                "where ss.score>=80");

        //将dataFrame的数据保存到good_student_info表中
        hiveContext.sql("DROP TABLE IF EXISTS good_student_info");
        result.saveAsTable("good_student_infos");

        //然后针对good_student_infos直接创建DataFrame
    /*    Row[] goodStudentInfos = hiveContext.table("good_student_infos").collect();
        for (Row goodStudentInfo : goodStudentInfos
        ) {
            System.out.println(goodStudentInfo);
        }*/
        sc.close();
    }
}
