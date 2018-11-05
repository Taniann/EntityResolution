package ua.kpi.tef;

import Jama.Matrix;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.mutable.WrappedArray;


import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

/**
 * Created by Таня on 03.11.2018.
 */
public class Main {
    public static void main(String[] args) {
/*        SparkConf conf = new SparkConf().setAppName("SQL Spark queries").setMaster("local");
        // create Spark Context
        SparkContext context = new SparkContext(conf);
        // create spark Session
        SparkSession sparkSession = new SparkSession(context);

        DataFrameReader dataFrameReader = sparkSession.read().option("header", true).option("inferSchema", "true");
        Dataset<Row> dfAmazon = dataFrameReader.csv("D:/Amazon.csv");
        Dataset<Row> dfGoogle = sparkSession.read().option("header", true).option("inferSchema", "true").csv("D:/GoogleProducts.csv");*/

/*        dfAmazon.show();
        dfAmazon.printSchema();
        dfGoogle.show();
        dfGoogle.printSchema();*/
        SparkConf conf = new SparkConf().setAppName("SQL Spark queries").setMaster("local");
        // create Spark Context
        SparkContext context = new SparkContext(conf);
        // create spark Session
        SparkSession sparkSession = new SparkSession(context);
        JavaRDD<String> amazonRDD = sparkSession.sparkContext()
                .textFile("D:/Amazon.txt", 1)
                .toJavaRDD();
        JavaRDD<String> googleRDD = sparkSession.sparkContext()
                .textFile("D:/Google.txt", 1)
                .toJavaRDD();

        // The schema is encoded in a string
        String schemaString = "id title description manufacturer price";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDDAmazon = amazonRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1],
                    attributes[2], attributes[3],
                    attributes[4].trim());
        });
        JavaRDD<Row> rowRDDGoogle = googleRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1],
                    attributes[2], attributes[3],
                    attributes[4].trim());
        });

        // Apply the schema to the RDD
        Dataset<Row> amazonDataFrame = sparkSession.createDataFrame(rowRDDAmazon, schema);
        Dataset<Row> googleDataFrame = sparkSession.createDataFrame(rowRDDGoogle, schema);


        Tokenizer tokenizer = new Tokenizer().setInputCol("description").setOutputCol("desc_words");
        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol("description")
                .setOutputCol("desc_words")
                .setPattern(" ");

       sparkSession.udf().register(
                "countTokens", (WrappedArray<?> words) -> words.size(), DataTypes.IntegerType);

        Dataset<Row> tokenizedGoogle = regexTokenizer.transform(googleDataFrame)
                .select("id", "title", "manufacturer", "price", "desc_words")
                .withColumn("tokens", callUDF("countTokens", col("desc_words")));

        Dataset<Row> tokenizedAmazon = regexTokenizer.transform(amazonDataFrame).
                select("id", "title", "manufacturer", "price", "desc_words")
                .withColumn("tokens", callUDF("countTokens", col("desc_words")));
/*      tokenizedAmazon.show();
        tokenizedAmazon.printSchema();

        tokenizedGoogle.show();
        tokenizedGoogle.printSchema();*/

        tokenizedAmazon.select(sum("tokens")).show();
        tokenizedGoogle.select(sum("tokens")).show();


        StopWordsRemover remover = new StopWordsRemover()
                .setInputCol("desc_words")
                .setOutputCol("filtered");

        Dataset<Row> filteredGoogle = remover
                .transform(tokenizedGoogle)
                .select("id", "title", "manufacturer", "price", "filtered")
                .withColumn("tokens", callUDF("countTokens", col("filtered")));

        Dataset<Row> filteredAmazon = remover
                .transform(tokenizedAmazon)
                .select("id", "title", "manufacturer", "price", "filtered")
                .withColumn("tokens", callUDF("countTokens", col("filtered")));

/*        filteredAmazon.show();
        filteredAmazon.printSchema();

        filteredGoogle.show();
        filteredGoogle.printSchema();*/

        filteredAmazon.select(sum("tokens")).show();
        filteredGoogle.select(sum("tokens")).show();

        HashingTF hashingTF = new HashingTF()
                .setInputCol("filtered")
                .setOutputCol("rawFeatures");

        Dataset<Row> featurizedGoogle = hashingTF.transform(filteredGoogle);
        Dataset<Row> featurizedAmazon = hashingTF.transform(filteredAmazon);

        IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
        IDFModel idfGoogleModel = idf.fit(featurizedGoogle);
        IDFModel idfAmazonModel = idf.fit(featurizedAmazon);

        Dataset<Row> rescaledGoogleData = idfGoogleModel
                .transform(featurizedGoogle)
                .select("id", "title","manufacturer", "price", "features");

       /* Dataset<Row> rescaledAmazonData = idfAmazonModel
                .transform(featurizedAmazon)
                .select("title", "manufacturer", "price", "features");
*/
       Dataset<Row> rescaledAmazonData = idfAmazonModel
                .transform(featurizedAmazon)
                .select("id", "title", "manufacturer", "price", "features")
                .withColumnRenamed("id", "a_id")
                .withColumnRenamed("title", "a_title")
                .withColumnRenamed("manufacturer", "a_manufacturer")
                .withColumnRenamed("price", "a_price")
                .withColumnRenamed("features", "a_features");

        Dataset<Row> merged = rescaledGoogleData.crossJoin(rescaledAmazonData);
        sparkSession.udf().register(
                "distance", (Vector f1, Vector f2) -> cosineSimilarity(f1.toArray(), f2.toArray()),DataTypes.DoubleType);

        merged.select("id", "a_id", "features", "a_features")
                .withColumn("dist", callUDF("distance", col("features"), col("a_features")))
                .filter("dist > 0.5")
                .show(false);
        merged.printSchema();

    }

    public static double cosineSimilarity(double[] A, double[] B) {
        if (A == null || B == null || A.length == 0 || B.length == 0 || A.length != B.length) {
            return 0.0;
        }

        double sumProduct = 0;
        double sumASq = 0;
        double sumBSq = 0;
        for (int i = 0; i < A.length; i++) {
            sumProduct += A[i]*B[i];
            sumASq += A[i] * A[i];
            sumBSq += B[i] * B[i];
        }
        if (sumASq == 0 && sumBSq == 0) {
            return 0.0;
        }
        return sumProduct / (Math.sqrt(sumASq) * Math.sqrt(sumBSq));
    }


    private static String cutToNWords(String line, int n) {
        StringBuilder result = new StringBuilder();
        String[] words = line.split(" ");

        if (words.length <= n) return line;

        for (int i = 0; i < n; i++) {
            result.append(words[i]).append(" ");
        }
        return result.toString().trim();
     }

}
