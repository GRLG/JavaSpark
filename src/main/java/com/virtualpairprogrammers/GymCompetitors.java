package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegressionModel;

public class GymCompetitors {

    public static void main(String[] args) {


        Logger.getLogger("orp.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("Gym Competitors")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> csvData = spark.read()
                    .option("header",true)
                    .option("inferSchema",true)
                    .csv("src/main/resources/GymCompetition.csv");

        csvData.printSchema();

        VectorAssembler vectorAssembler;
        vectorAssembler = new VectorAssembler();
        vectorAssembler.setInputCols(new String[] {"Age", "Height", "Weight"});
        vectorAssembler.setOutputCol("features");
        Dataset<Row> csvDataWithFeatures = vectorAssembler.transform(csvData);
        Dataset<Row> Data = csvDataWithFeatures.select("NoOfReps","features").withColumnRenamed("NoOfReps","label");
        Data.show();

        LinearRegression linearRegressionModel = new LinearRegression();
        LinearRegressionModel model =  linearRegressionModel.fit(Data);
        System.out.println(model.intercept());
        System.out.println(model.coefficients());

        model.transform(Data).show();

        spark.close();
    }
}
