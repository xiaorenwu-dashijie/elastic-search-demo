package com.es.demo.vo;


import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import org.apache.lucene.document.FieldType;
import org.jboss.logging.Field;

import java.text.DateFormat;
import java.util.Date;

/**
 * @Description:
 * @Param:
 * @Return:
 * @Author: qjc
 * @Date: 2019/10/18
 */
@Data   //IDEA需要安装lombok：https://www.cnblogs.com/java-spring/p/9797560.html
public class User {

    private String name;

    private int age;

    private Double money;

    private String address;

    private String birthday;
}
