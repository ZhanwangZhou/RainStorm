package test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;


/*
File generator for test purpose.
int: fileNumber, int fileSize, str: filepath food
output: String; "food_1.txt, food_2.txt, food_3.txt"
 */
public class FileGenerator {

    public static void main(String[] args) {
        int numberOfFiles = 1000;
        int fileSizeKB = 40;
        String filePath ="archive/per40/";

        String generatedFiles = generateFiles(numberOfFiles, fileSizeKB, filePath);
        System.out.println("Generated files: ");
        System.out.println(generatedFiles);
    }
    public FileGenerator() {

    }

    public static String generateFiles(int numberOfFiles, int fileSizeKB, String filePath) {
        String[] pathSplit = filePath.split("/");
        String filename = pathSplit[pathSplit.length - 1];

        StringBuilder sb = new StringBuilder();
        Path dirPath = Paths.get(filePath);

        try {
            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
            }

            for (int i = 1; i <= numberOfFiles; i++) {
                String content = generateRandomText(fileSizeKB * 1024);
                Path file = dirPath.resolve(filename + "_" + i + ".txt");
                Files.write(file, content.getBytes("UTF-8"));

                sb.append(filePath).append(filename).append("_").append(i).append(".txt");
                if (i < numberOfFiles) {
                    sb.append(",");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return sb.toString();
    }

    private static String generateRandomText(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ";
        Random rand = new Random();
        StringBuilder text = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            text.append(chars.charAt(rand.nextInt(chars.length())));
        }

        return text.toString();
    }
}