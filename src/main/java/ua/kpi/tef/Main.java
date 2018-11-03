package ua.kpi.tef;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

/**
 * Created by Таня on 03.11.2018.
 */
public class Main {
    public static void main(String[] args) throws IOException {
/*        BufferedReader CSVFile1 = new BufferedReader(new FileReader("D:/Amazon.csv"));
        String dataRow1 = CSVFile1.readLine();
        while (dataRow1 != null) {
            String[] dataArray1 = dataRow1.split(",\"");
            for (String item1 : dataArray1)
            {
                System.out.println(item1);
            }

            dataRow1 = CSVFile1.readLine(); // Read next line of data.
        }
        CSVFile1.close();*/

        SparkConf conf = new SparkConf().setAppName("SQL Spark queries").setMaster("local");
        // create Spark Context
        SparkContext context = new SparkContext(conf);
        // create spark Session
        SparkSession sparkSession = new SparkSession(context);

        DataFrameReader dataFrameReader = sparkSession.read().option("header", true);
        Dataset<Row> dfAmazon = dataFrameReader.csv("D:/Amazon.csv");
        Dataset<Row> dfGoogle = sparkSession.read().option("header", true).csv("D:/GoogleProducts.csv");
        List<Row> amazonData = dfAmazon.collectAsList();
        List<Row> googleData = dfGoogle.collectAsList();
        int counterTen = 0;
        int counterFive = 0;
        for(Row amazon : amazonData) {
            String[] strA = amazon.toString().split(",");
            if (strA[2].equals("null")) continue;
            for (Row google : googleData) {
                String[] strG = google.toString().split(",");
                if (strG[2].equals("null")) continue;
                if (cutTo10Words(strA[2]).equals(cutTo10Words(strG[2]))) {
                    System.out.println(strA[0] + " " + strG[0]);
                    counterTen++;
                }
            }
        }
        System.out.println("//////////////////////////////////////////////////////////////////////");
        for(Row amazon : amazonData) {
            String[] strA = amazon.toString().split(",");
            if (strA[2].equals("null")) continue;
            for (Row google : googleData) {
                String[] strG = google.toString().split(",");
                if (strG[2].equals("null")) continue;
                if (cutTo5Words(strA[2]).equals(cutTo5Words(strG[2]))) {
                    System.out.println(strA[0] + " " + strG[0]);
                    counterFive++;
                }
            }
        }

        System.out.println(counterTen);
        System.out.println(counterFive);

    }

    private static String cutTo10Words(String line) {
        StringBuilder result = new StringBuilder();
        String[] words = line.split(" ");

        if (words.length <= 10) return line;

        for (int i = 0; i < 10; i++) {
            result.append(words[i]).append(" ");
        }
        return result.toString().trim();
     }

    private static String cutTo5Words(String line) {
        StringBuilder result = new StringBuilder();
        String[] words = line.split(" ");

        if (words.length <= 5) return line;

        for (int i = 0; i < 5; i++) {
            result.append(words[i]).append(" ");
        }
        return result.toString().trim();
    }
}
