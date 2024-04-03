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
        dataset.show();
        System.out.println(dataset.count() +" Registros ");

        Row firstRow = dataset.first();

        String subject = firstRow.get(2).toString();
        System.out.println(subject);

        subject = firstRow.getAs("subject").toString();
        System.out.println(subject);

        int year = Integer.parseInt(firstRow.getAs("year"));
        System.out.println(year);

        Dataset<Row> moderArtResult = dataset.filter("subject='Modern Art' AND year >=2007");
        moderArtResult.show();

       Dataset<Row> moderArtResult2 = dataset.filter((FilterFunction<Row>) r -> r.getAs("subject").equals("Mordern Arts")
                                                                                && Integer.parseInt(r.getAs("year")) >= 2007);
       moderArtResult2.show();


        spark.close();



    }
}