package com.typeTran;

import com.util.Student;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * 使用反射的方式将RDD转换为DataFrame
 */
public class RDDToDataFrame {
    public static void main(String[] args) {
        //创建普通的RDD
        //local单核单线程，local[2]使用两个核，local[*]使用所有的核
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("RDDToDataFrame2");
        conf.set("spark.driver.memory","512m").set("spark.testing.memory","1073740000");
        //conf.set("spark.driver.host","test");
        //conf.set("spark.driver.bindAddress","localhost");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> lines = sc.textFile("D:\\ideaProject2\\spark-job\\students.txt");

        JavaRDD<Student> students = lines.map(new Function<String, Student>() {

            private static final long serialVersionUID = 1L;

            public Student call(String line) throws Exception {
                String[] split = line.split(",");
                Student student = new Student();
                student.setId(Integer.valueOf(split[0].trim()));
                student.setAge(Integer.valueOf(split[2].trim()));
                student.setName(String.valueOf(split[1]));
                return student;
            }
        });


        // 使用反射的方式将RDD转换为DataFrame
        // 使用Student.class作为一个反射，因为Student.class本身就是一个反射的应用
        //todo 这里要求Student.class必须可进行序列化
        //todo Dataset<Row>=DataFrame
        DataFrame studentDF = sqlContext.createDataFrame(students, Student.class);

        //得到dataFrame后，可以注册一个临时表，然后对其中的数据进行sql处理
        studentDF.registerTempTable("student");

        //执行sql语句查询
        DataFrame teenagerDF = sqlContext.sql("select * from student where age <= 18");
        //可以直接将这个dataframe打印
        teenagerDF.show();
        //查询出来的结果转换成RDD
        JavaRDD<Row> teenagerJavaRDD = teenagerDF.toJavaRDD();

        //将RDD里的数据，进行映射，映射为student
        JavaRDD<Student> teenagerStudentRDD = teenagerJavaRDD.map(new Function<Row, Student>() {
            private static final long serialVersionUID = 1L;

            public Student call(Row row) throws Exception {
                Student stu = new Student();
                stu.setAge(row.getInt(0));
                stu.setId(row.getInt(1));
                stu.setName(row.getString(2));
                return stu;
            }
        });

        //将数据collect回来，打印输出
        List<Student> collects = teenagerStudentRDD.collect();
        for (Student collect : collects
        ) {
            System.out.println(collect);
        }

    }
}
