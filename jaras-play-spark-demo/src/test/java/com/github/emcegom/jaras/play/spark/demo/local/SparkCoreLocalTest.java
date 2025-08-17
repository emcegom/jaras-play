package com.github.emcegom.jaras.play.spark.demo.local;

import com.github.emcegom.jaras.play.common.util.FileUtils;
import com.github.emcegom.jaras.play.common.util.JsonUtils;
import com.github.emcegom.jaras.play.spark.demo.model.User;
import com.github.emcegom.jaras.play.spark.demo.test.SparkFileUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Tuple2;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class SparkCoreLocalTest {
    private SparkConf sparkConf;
    private JavaSparkContext sc;

    @TempDir
    private Path tmpDir;

    @BeforeEach
    public void setUp() throws ClassNotFoundException {
        sparkConf = new SparkConf()
                .setAppName("SparkCoreLocalTest")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{Class.forName("com.github.emcegom.jaras.play.spark.demo.model.User")})
                .setMaster("local[*]");
        sc = new JavaSparkContext(sparkConf);
    }

    @AfterEach
    public void tearDown() {
        if (sc != null) {
            sc.stop();
        }
    }


    @Test
    public void testSparkCoreLocalFile() throws Exception {
        String inputPath = tmpDir.toAbsolutePath() + File.separator + "testSparkCoreLocalFile_input.txt";
        String outputPath = tmpDir.toAbsolutePath() + File.separator + "testSparkCoreLocalFile_output.txt";

        FileUtils.createFileIfNotExist(inputPath,
                "Hello World\n" +
                        "This is a test\n" +
                        "Testing Spark Core Local File\n" +
                        "Testing Spark Core Local File\n" +
                        "Testing Spark Core Local File\n");

        JavaRDD<String> lineRDD = sc.textFile(inputPath);
        lineRDD.collect().forEach(log::info);
        lineRDD.map(s -> s + "||");
        lineRDD.collect().forEach(log::info);
        lineRDD.saveAsTextFile(outputPath);
//        TimeUnit.SECONDS.sleep(100000);

        File outputDir = new File(outputPath);
        File[] partFiles = outputDir.listFiles(f -> f.getName().startsWith("part-"));
        assert partFiles != null;
        log.info("Number of part files: {}", partFiles.length);

        List<String> allLines = SparkFileUtils.readAllLinesFromSparkOutputDir(outputDir);
        allLines.forEach(log::info);
    }

    @Test
    public void testSparkCoreLocalDistinct() throws InterruptedException {
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 6), 2);
        JavaRDD<Integer> distinct = javaRDD.distinct();
        distinct.collect().forEach(e -> log.info(e + ""));

    }

    @Test
    public void testSparkCoreLocalSerialization() throws InterruptedException {
        JavaRDD<User> javaRDD = sc.parallelize(Arrays.asList(new User("John", 30), new User("Mary", 25)), 2);

        JavaRDD<User> mapRDD = javaRDD.map(r -> new User(r.getName(), r.getAge() + 1));

        mapRDD.collect().forEach(e -> log.info(JsonUtils.toJson(e)));

    }

    @Test
    public void testSparkCoreLocalCase01() throws Exception {
        String inputPath = tmpDir.toAbsolutePath() + File.separator + "testSparkCoreLocalFile_input.txt";

        FileUtils.createFileIfNotExist(inputPath,
                "Hello World\n" +
                        "This is a test\n" +
                        "Testing Spark Core Local File\n" +
                        "Testing Spark Core Local File\n" +
                        "Testing Spark Core Local File\n");

        JavaRDD<String> lineRDD = sc.textFile(inputPath);
        log.info(lineRDD.toDebugString());

        JavaRDD<String> wordRDD = lineRDD.flatMap(r -> Arrays.asList(r.split(" ")).iterator());
        JavaPairRDD<String, Integer> tupleRDD = wordRDD.mapToPair(w -> new Tuple2<>(w, 1));
        log.info(tupleRDD.toDebugString());

        JavaPairRDD<String, Integer> coalesce = tupleRDD.coalesce(1);
        JavaPairRDD<String, Integer> wordCountRDD = coalesce.reduceByKey(Integer::sum);
        log.info(wordCountRDD.toDebugString());

        wordCountRDD.collect().forEach(e -> log.info("{}", e));
        wordCountRDD.collect().forEach(e -> log.info("{}", e));
        Thread.sleep(100000);
    }



    @Test
    public void testSparkCoreLocalCase02() throws Exception {
        String inputPath = tmpDir.toAbsolutePath() + File.separator + "testSparkCoreLocalFile_input.txt";

        FileUtils.createFileIfNotExist(inputPath,
                "Hello World\n" +
                        "This is a test\n" +
                        "Testing Spark Core Local File\n" +
                        "Testing Spark Core Local File\n" +
                        "Testing Spark Core Local File\n");

        sc.setCheckpointDir(tmpDir + File.separator + "ck");
        JavaRDD<String> lineRDD = sc.textFile(inputPath);
        log.info(lineRDD.toDebugString());

        JavaRDD<String> wordRDD = lineRDD.flatMap(r -> Arrays.asList(r.split(" ")).iterator());
        JavaPairRDD<String, Integer> tupleRDD = wordRDD.mapToPair(w -> new Tuple2<>(w, 1));
        log.info(tupleRDD.toDebugString());

        tupleRDD.cache();
        tupleRDD.checkpoint();
        tupleRDD.collect().forEach(e -> log.info("{}", e));
        log.info(tupleRDD.toDebugString());
        tupleRDD.collect().forEach(e -> log.info("{}", e));
        tupleRDD.collect().forEach(e -> log.info("{}", e));

        Thread.sleep(1000000);
    }
}
