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

public class CSVReader {

    public static void main(String[] args) throws IOException {

        Path path = Paths.get("/Users/uma6/IdeaProjects/test_api/test_csv.csv");
        Stream<String> lines = Files.lines(path);
        lines.forEach(line -> {
            System.out.println(line);
        });

//
        try (BufferedReader csv = Files.newBufferedReader(path)) {
            // １行めを見出しとして読み込む
            final String head = csv.readLine();
            System.out.println("head: " + head);
            // カラム名の配列
            final String[] flds = head.split(",\\s*");
//            System.out.println("flds: " + );
            Arrays.stream(flds).forEach(s -> System.out.println("flds: " + s));

            // データ行読み込み
            // 2行目から Stream にして読み込む
//            System.out.println("iei " + csv.lines().collect(Collectors.toList()));
//            List<String> list = new ArrayList<>();
            List<String> strList = csv.lines().collect(Collectors.toList());
            System.out.println("sss " + strList.toArray()[1]);


        } catch (Exception e) {
            System.out.println("e " + e);
        }

    }

}
