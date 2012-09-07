package com.jayway.hadoop.fress.spring;

import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Run {


    public static void main(String args[]) {
        new ClassPathXmlApplicationContext("/spring/hadoop-config.xml");
    }
}
