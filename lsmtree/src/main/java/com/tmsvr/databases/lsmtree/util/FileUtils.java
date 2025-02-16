package com.tmsvr.databases.lsmtree.util;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.NoSuchFileException;
import java.util.function.Function;

@Slf4j
public class FileUtils {
    public static <T> void saveToDisk(Path filePath, T data, Function<T, String> dataProcessor) throws IOException {
        String processedData = dataProcessor.apply(data);
        Files.writeString(filePath, processedData);
    }

    public static <T> T loadFromDisk(Path filePath, Function<String, T> dataProcessor, T defaultValue) throws IOException {
        try {
            String rawData = Files.readString(filePath);
            return dataProcessor.apply(rawData);
        } catch (NoSuchFileException e) {
            log.info("File '" + filePath + "' is empty or not present, returning default value");
            return defaultValue;
        } catch (IOException e) {
            throw new IOException("Failed to load data from file: " + filePath, e);
        }
    }
}
