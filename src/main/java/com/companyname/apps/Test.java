package com.companyname.apps;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

class Entity {
    public Integer aaa;
    public String bbb;
}

public class Test {
    public static void main(String[] args) throws IOException {
//        ObjectMapper mapper = new ObjectMapper();
//        Entity entity = mapper.readValue("{\"aaa\":1,\"bbb\":\"hoge\"}", Entity.class);
//        System.out.println("entity " + entity.aaa);

        Path p = Paths.get("/", "/dir", "path/");
        System.out.println("aa: " + p.toString());



    }
}
