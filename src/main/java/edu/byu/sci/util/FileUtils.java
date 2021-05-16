package edu.byu.sci.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class FileUtils {
    private FileUtils() {
        // Ignore
    }

    public static String stringFromFile(File file) {
        StringBuilder contentBuilder = new StringBuilder();

        try ( Stream<String> stream = Files.lines(Paths.get(file.getPath()),
                StandardCharsets.UTF_8)) {
            stream.forEach(s -> contentBuilder.append(s).append("\n"));
        } catch (IOException e) {
            Logger.getLogger(FileUtils.class.getName()).log(Level.SEVERE, () -> "Unable to read file: " + e);
            System.exit(-1);
        }

        return contentBuilder.toString();
    }

    public static void writeStringToFile(String content, File file) {
        try {
            try ( FileOutputStream stream = new FileOutputStream(file)) {
                stream.write(content.getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            Logger.getLogger(FileUtils.class.getName()).log(Level.SEVERE, () -> "Unable to write file: " + e);
            System.exit(-1);
        }
    }
}
