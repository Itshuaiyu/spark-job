package com.typeTran;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * 用编程方式动态创建元数据
 * 将RDD转化为DataFrame
 */
public class RDDToDataFrame2 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("RDDToDataFrame2");
        conf.set("spark.driver.memory", "512m").set("spark.testing.memory", "1073740000");


        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("D:\\ideaProject2\\spark-job\\students.txt");
        //1.创建一个普通的RDD，必须将其转化为RDD<row>格式
        JavaRDD<Row> studentRdd = lines.map(new Function<String, Row>() {
            private static final long serialVersionUID = 1L;
            public Row call(String line) throws Exception {
                String[] split = line.split(",");

                return RowFactory.create(Integer.valueOf(split[0]),split[1],Integer.valueOf(split[2]));
            }
        });

        SQLContext sqlContext = new SQLContext(sc);

        //2.动态构造元数据
        //StructField结构体对象
        //为了从mysql数据库或者配置文件中获取元数据，不是固定的
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        StructType structType = DataTypes.createStructType(structFields);


        //3.使用动态元数据将RDD转化为DataFrame
        DataFrame studentDF = sqlContext.createDataFrame(studentRdd, structType);

        studentDF.registerTempTable("students");

        DataFrame teenagerDF = sqlContext.sql("select * from students where age >18");

        JavaRDD<Row> rowJavaRDD = teenagerDF.toJavaRDD();

        List<Row> collects = rowJavaRDD.collect();

        for (Row collect : collects
                ) {
            System.out.println(collect);
        }

    }
}
