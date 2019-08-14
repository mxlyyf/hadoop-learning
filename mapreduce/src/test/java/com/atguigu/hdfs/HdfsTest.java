package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

public class HdfsTest {
    public Configuration conf;
    public FileSystem fs;

    @Before
    public void before() throws Exception{
        conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://192.168.213.101:9000");
        fs = FileSystem.get(conf);
        //fs = FileSystem.get(new URI("hdfs://192.168.213.101:9000"),conf);
    }

    @After
    public void after() throws Exception{
        fs.close();
    }

    @Test//上传文件
    public void test01() throws Exception{
        Path src = new Path("D:\\BaiduyunDiskDownload\\jdk-8-windows-x64.exe");
        Path desc = new Path("/Test Hdfs Api Operation/jdk-8-windows-x64.exe");
        if (fs.exists(desc)){
            //fs.delete(desc,true);
        }
        fs.copyFromLocalFile(false, src, desc);
    }

    @Test//下载文件
    public void test02() throws Exception{
        Path src = new Path("/flume/20190604/20/hivelogs-.1559704905111");
        Path desc = new Path("D:/tmp/logs/log2.txt");
        fs.copyToLocalFile(false, src, desc,true);
    }

    @Test//删除文件（夹）
    public void test03() throws Exception{
        fs.delete(new Path("/app-2019-06-15.log"),true);
    }

    @Test//创建文件（夹）
    public void test04() throws Exception{
        fs.mkdirs(new Path("/Test Hdfs Api Operation"));
    }

    @Test//通过IO流上传文件
    public void test05() throws Exception{
        //Path src = new Path("D:\\tmp\\logs\\log2.txt");
        Path dest = new Path("/Test Hdfs Api Operation/log2.txt");
        //client需要获得待上传的文件
        FileInputStream fis = new FileInputStream("D:\\tmp\\logs\\log2.txt");
        //相当于client，创建输出流
        FSDataOutputStream fos = fs.create(dest, true);
        //流对拷
        IOUtils.copyBytes(fis,  fos, conf, true);
    }

    @Test//通过IO流下载文件
    public void test06() throws Exception{
        FSDataInputStream fis = fs.open(new Path("/flume/upload/20190604/20/upload-.1559706343994"));
        FileOutputStream fos = new FileOutputStream("d:/tmp/logs/log3.log");
        IOUtils.copyBytes(fis,fos,conf,true);
    }
}
