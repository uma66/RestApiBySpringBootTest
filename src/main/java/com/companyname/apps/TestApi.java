package com.companyname.apps;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

// SpringBootServletInitializerの継承はwarファイルを作って別のtomcatにデプロイするために必要
//  参考 => https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle/#howto-create-a-deployable-war-file
@SpringBootApplication
public class TestApi extends SpringBootServletInitializer {

    // warファイルを作って別のtomcatにデプロイするために必要
    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(TestApi.class);
    }

    public static void main(String[] args) {
        SpringApplication.run(TestApi.class, args);
    }

}

