package com.github.emcegom.jaras.play.spark.demo.local;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

@Slf4j
public class SparkLocalDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("SparkLocalExample")  // 应用名
                .setMaster("local[*]");           // local[*] 表示使用所有 CPU 核心

        // 2. 创建 JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 创建 RDD 示例
        JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        // 4. 执行简单操作
        int sum = numbers.reduce(Integer::sum);
        log.info("Sum of numbers: " + sum);

        // 5. 关闭上下文
        sc.close();
    }
}
