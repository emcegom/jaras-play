package com.github.emcegom.jaras.play.spark.demo.local;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLDemo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkSQLLocalExample")
                .master("local[*]")
                .getOrCreate();

        // 创建 DataFrame
        Dataset<Row> df = spark.read().json("C:\\Users\\otis\\projects\\jaras-play\\jaras-play-spark-demo\\src\\main\\resources\\data.json");
        df.show();

        spark.stop();
    }
}
