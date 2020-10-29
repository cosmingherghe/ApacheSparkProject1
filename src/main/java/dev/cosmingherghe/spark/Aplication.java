package dev.cosmingherghe.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

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

        /* Add a new column called full name with the concatenation
        of the first and last name together & add static concat import + all functions.

        * Transforming the data and the data from object.
         */
        df = df.withColumn("full_name",
                concat(df.col("last_name"),
                        lit(", "),
                        df.col("first_name")));
        df.show();

        // Another transformation - We only want to see the comments that have numbers in them.
        df = df.filter(df.col("comment").rlike("\\d+"))
                .orderBy(df.col("last_name").asc());
        df.show();

        // Saving data into a database
        String dbConnectionUrl = "jdbc:postgresql://localhost/course_data"; // <<- You need to create this database
        Properties prop = new Properties();
        prop.setProperty("driver", "org.postgresql.Driver");
        prop.setProperty("user", "postgres");
        prop.setProperty("password", "password"); // <- The password you used while installing Postgres

        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "project1", prop);
    }
}
