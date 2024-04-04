package org.example;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql .*;
import scala.Function1;
import scala.reflect.internal.Symbols;


public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");

        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true)
                .csv("src/main/resources/exams/students.csv");
   //     dataset.show();
        System.out.println(dataset.count() +" Registros ");

        Row firstRow = dataset.first();

        String subject = firstRow.get(2).toString();
        System.out.println(subject);

        subject = firstRow.getAs("subject").toString();
        System.out.println(subject);

        int year = Integer.parseInt(firstRow.getAs("year"));
        System.out.println(year);

        Dataset<Row> moderArtResult = dataset.filter("subject='Modern Art' AND year >=2007");
      //  moderArtResult.show();

       Dataset<Row> moderArtResult2 = dataset.filter((FilterFunction<Row>) r -> r.getAs("subject").equals("Modern Art")
                                                                                && Integer.parseInt(r.getAs("year")) >= 2007);
    //   moderArtResult2.show();

       Column subjectCol = dataset.col("subject");
       Column yearCol = dataset.col("year");

       Dataset <Row> moderArtResult3 = dataset.filter(subjectCol.equalTo("Modern Art")
                                                                               .and(yearCol.geq(2007)) );
  //     moderArtResult3.show();

       dataset.createOrReplaceTempView("my_students");

//       Dataset<Row> result = spark.sql("select * from my_students where subject ='French' ");
//       Dataset<Row> result = spark.sql("select score, year  from my_students where subject ='French' ");

//        Dataset<Row> result = spark.sql("select max(score)  from my_students where subject ='French' ");
//        Dataset<Row> result = spark.sql("select avg(score)  from my_students where subject ='French' ");
        Dataset<Row> result = spark.sql("select distinct(year)  from my_students where subject ='French' ");
        spark.cr
        result.show();
        spark.close();



    }
}