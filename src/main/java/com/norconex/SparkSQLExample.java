package com.norconex;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLExample {
    public static void main(String[] args) {
        // Create a SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL Example")
                .config("spark.master", "local")
                .getOrCreate();

        // Read data from a CSV file
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("C:\\workspace\\intellij\\Spark\\JDBCExampleProject\\src\\main\\resources\\csvfile.csv");

        // Show the schema of the DataFrame
        df.printSchema();

        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("people");

        // Run a SQL query
        Dataset<Row> sqlDF = spark.sql("SELECT name, age FROM people WHERE age > 30");

        // Show the results
        sqlDF.show();

        // Perform some transformations
        Dataset<Row> transformedDF = df.filter("age > 30")
                .groupBy("age")
                .count();

        // Show the transformed data
        transformedDF.show();

        // Write the results back to a file
        transformedDF.write().format("csv")
                .option("header", "true")
                .save("path/to/output/csvfile.csv");

        // Stop the SparkSession
        spark.stop();
    }
}
