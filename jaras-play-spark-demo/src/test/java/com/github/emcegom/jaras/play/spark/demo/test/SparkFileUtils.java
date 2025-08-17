package com.github.emcegom.jaras.play.spark.demo.test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SparkFileUtils {
    public static List<String> readAllLinesFromSparkOutputDir(File dir) throws IOException {
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IllegalArgumentException("目录不存在或不是目录: " + dir.getAbsolutePath());
        }

        return Arrays.stream(Objects.requireNonNull(dir.listFiles(f -> f.getName().startsWith("part-"))))
                .flatMap(file -> {
                    try {
                        return Files.lines(file.toPath());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .collect(Collectors.toList());
    }
}
