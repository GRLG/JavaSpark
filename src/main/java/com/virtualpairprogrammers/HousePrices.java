package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePrices {
    public static void main(String[] args) {


        Logger.getLogger("orp.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("HousePrices")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> csvData = spark.read()
                .option("header",true)
                .option("inferSchema",true)
                .csv("src/main/resources/kc_house_data.csv");

        csvData.printSchema();

        VectorAssembler vectorAssembler;
        vectorAssembler = new VectorAssembler()
                .setInputCols(new String[] {"bedrooms", "bathrooms", "sqft_living","sqft_lot","floors","grade"})
                .setOutputCol("features");
        Dataset<Row> modelInputData = vectorAssembler.transform(csvData)
                .select("price","features")
                .withColumnRenamed("price","label");

        Dataset<Row>[] trainingAndTestData =  modelInputData.randomSplit(new double[]{0.8,0.2});
        Dataset<Row> trainingData = trainingAndTestData[0];
        Dataset<Row> testData = trainingAndTestData[1];

        LinearRegressionModel model = new LinearRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8)
                .fit(trainingData);
        model.transform(testData).show();

        System.out.println("the training data r2 value is:" + model.summary().r2() + " and the rmse is:" + model.summary().rootMeanSquaredError());
        System.out.println("the test data r2 value is:" + model.evaluate(testData).r2() + " and the rmse is:" + model.evaluate(testData).rootMeanSquaredError());





        spark.close();
    }
}
