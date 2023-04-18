package com.util;

import lombok.Data;

import java.io.Serializable;

/**
 * 学生javabean
 */
@Data
//需要进行序化
public class Student implements Serializable {
    private int id;
    private String name;
    private int age;
}
