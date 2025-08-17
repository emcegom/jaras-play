package com.github.emcegom.jaras.play.common.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FileUtils {

    public static void createFileIfNotExist(String filePath, String defaultContent) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            // 创建父目录
            File parent = file.getParentFile();
            if (!parent.exists()) {
                parent.mkdirs();
            }
            // 写入内容
            try (FileWriter writer = new FileWriter(file)) {
                if (defaultContent != null) {
                    writer.write(defaultContent);
                }
            }
        }
    }

    public static void deleteIfExists(String filePath) {
        File file = new File(filePath);
        if (file.exists()) {
            deleteRecursively(file);
        }
    }

    private static void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] subs = file.listFiles();
            if (subs != null) {
                for (File sub : subs) {
                    deleteRecursively(sub);
                }
            }
        }
        file.delete();
    }


}
