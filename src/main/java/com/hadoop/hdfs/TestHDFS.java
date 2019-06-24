package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

/**
 * @author Chester
 * @create 2019-06-24 17:00
 * @desc the demo of HDFS
 **/
public class TestHDFS {
    public Configuration conf = null;
    public FileSystem fs = null;
    @Before
    public void conn(){
        conf = new Configuration();
        try {
            fs = FileSystem.get(URI.create("hdfs://mycluster/"), conf, "god");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void mkdir(){
        Path dir = new Path("/testM");
        try {
            if (fs.exists(dir));
            fs.delete(dir,true);
            fs.mkdirs(dir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void upload() throws  Exception{
        BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(new File("C:/Users/mayn/Desktop/bigdata/hadoop/HDFS.doc")));
        Path outfile = new Path("/testA/HDFS.doc");
        FSDataOutputStream fsDataOutputStream = fs.create(outfile);
        IOUtils.copyBytes(inputStream,fsDataOutputStream,conf,true);
    }

    @After
    public void after(){
        try {
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
