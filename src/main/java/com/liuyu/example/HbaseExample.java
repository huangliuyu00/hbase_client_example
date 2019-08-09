package com.liuyu.example;

import com.liuyu.service.HBaseService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

/**
 * @author huangliuyu
 * @description
 * @date 2019-08-09
 */
public class HbaseExample {
    public static void main(String[] args) {
        Configuration configuration= HBaseConfiguration.create();
        HBaseService hBaseService=new HBaseService();
        hBaseService.setConfig(configuration);
        try {
            hBaseService.queryAll("member");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
