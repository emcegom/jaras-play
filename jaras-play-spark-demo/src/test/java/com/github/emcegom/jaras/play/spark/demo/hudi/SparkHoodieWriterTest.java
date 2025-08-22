package com.github.emcegom.jaras.play.spark.demo.hudi;

import com.github.emcegom.jaras.play.spark.demo.test.SparkFileUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.*;

@Slf4j
public class SparkHoodieWriterTest {
    @TempDir
    public Path tempDir;

    @AfterEach
    @EnabledOnOs(OS.WINDOWS)
    void cleanUp() {
        System.gc();
    }

    @ParameterizedTest
    @ValueSource(strings = {"COPY_ON_WRITE"
//            , "MERGE_ON_READ"
    })
    public void testSparkHoodieWriter(String tableType) throws Exception {
        String parquetBasePath = SparkFileUtils.toUriPath(tempDir.resolve("parquet_input_" + tableType));
        String hudiBasePath = SparkFileUtils.toUriPath(tempDir.resolve("hudi_table_" + tableType));
        ;
        writeParquetData(parquetBasePath, 10000);

        SparkSession sparkReader = SparkSession.builder()
                .appName("HudiWriter-" + tableType)
                .master("local[4]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        try {
            Map<String, String> hudiOptions = new HashMap<>();
            hudiOptions.put("hoodie.table.name", "hudi_table_" + tableType);
            hudiOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "id");
            hudiOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "dt");
            hudiOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "ts");
            hudiOptions.put(HoodieWriteConfig.TBL_NAME.key(), "hudi_table_" + tableType);
            hudiOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), tableType);


            Dataset<Row> parquetDF = sparkReader.read().parquet(parquetBasePath);

            parquetDF.write()
                    .format("hudi")
                    .options(hudiOptions)
                    .mode(SaveMode.Overwrite)
                    .save(hudiBasePath);

            Dataset<Row> hudiDF = sparkReader.read().format("hudi").load(hudiBasePath + "/*/*");
            hudiDF.show(50, false);
            long total = hudiDF.count();
            log.info("[Reader] 读取{}条记录，总数: {}", total, hudiBasePath);
        } finally {
            sparkReader.stop();
            sparkReader.close();
        }
    }

    public void writeParquetData(String basePath, int numRecords) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("ParquetWriter")
                .master("local[*]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        try {
            int batchSize = 5000;
            for (int i = 1; i <= numRecords; i += batchSize) {
                int currentBatchSize = Math.min(batchSize, numRecords - i + 1);
                List<DataRecord> data = generateData(currentBatchSize);
                Dataset<Row> df = spark.createDataFrame(data, DataRecord.class);

                df.write()
                        .mode(SaveMode.Append)
                        .partitionBy("dt")
                        .parquet(basePath);
                df.show(20);
                log.info("[Writer] 追加写入{}条记录到 Parquet: {}", numRecords, basePath);
            }
        } finally {
            spark.stop();
            spark.close();
        }
    }

    @Data
    @AllArgsConstructor
    public static class DataRecord {
        private String id;
        private String name;
        private int age;
        private String phone;
        private Long ts;
        private String dt;
    }

    public List<DataRecord> generateData(int numRecords) {
        List<DataRecord> data = new ArrayList<>();
        Random rand = new Random();
        String[] names = {"Alice", "Bob", "Cathy", "David", "Eva"};
        String[] dates = {"2025-08-20", "2025-08-21", "2025-08-22", "2025-08-23"};
        for (int i = 0; i < numRecords; i++) {
            String id = UUID.randomUUID().toString();
            String name = names[rand.nextInt(names.length)];
            int age = rand.nextInt(100);
            String phone = 10000000000L + rand.nextLong() + "";
            long ts = System.currentTimeMillis() + rand.nextInt(10000);
            String dt = dates[rand.nextInt(dates.length)];
            data.add(new DataRecord(id, name, age, phone, ts, dt));
        }
        return data;
    }
}
