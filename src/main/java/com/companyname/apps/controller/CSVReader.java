package com.companyname.apps.controller;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.logging.Logger;


public class CSVReader {

    private static final Logger logger = Logger.getLogger(CSVReader.class.getName());

    public static void main(String[] args) throws IOException {

        Path path = Paths.get("/Users/uma6/IdeaProjects/test_api/test_csv.csv");
        Stream<String> lines = Files.lines(path);
        lines.forEach(line -> {
            logger.info(line);
        });

        try (BufferedReader csv = Files.newBufferedReader(path)) {
            // １行めを見出しとして読み込む
            final String head = csv.readLine();
            logger.info("head: " + head);
            // カラム名の配列
            final String[] flds = head.split(",\\s*");
//            System.out.println("flds: " + );
            Arrays.stream(flds).forEach(s -> System.out.println("flds: " + s));

            // データ行読み込み
            // 2行目から Stream にして読み込む
//            System.out.println("iei " + csv.lines().collect(Collectors.toList()));
//            List<String> list = new ArrayList<>();
            List<String> strList = csv.lines().collect(Collectors.toList());
            logger.info("sss " + strList.toArray()[1]);


        } catch (Exception e) {
            logger.severe("e " + e);
        }

    }

}
