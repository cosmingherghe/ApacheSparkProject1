package dev.cosmingherghe.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Aplication {

    public static void main(String args[]) {

        // Create a session
        SparkSession spark = new SparkSession.Builder()
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();

        // Get data
        /*
        We're going to need to specify a distributed file system here such
        as DFS the Hadoop distributed file system or something like the Amazon S3 bucket.
         */
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load("src/main/resources/name_and_comments.txt");

        //df > data frame
        df.show();

        // Show only 2 columns
        df.show(2);
    }
}
