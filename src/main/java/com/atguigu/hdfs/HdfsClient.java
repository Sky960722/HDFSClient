package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://hadoop102:8020");
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        String user = "atguigu";
        fs = FileSystem.get(uri, conf, user);
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {
        // 2 创建目录
        fs.mkdirs(new Path("/henruiyan/"));
    }

    //上传

    /**
     * 参数优先级
     * hdfs-default.xml => hdfs-site.xml => 在项目资源目录下的配置文件 =》 代码里面的配置
     *
     * @throws IOException
     */
    @Test
    public void testPut() throws IOException {
        fs.copyFromLocalFile(false, true, new Path("D:\\suwukong.txt"), new Path("hdfs://hadoop102/xiyou/huaguoshan/sunwukong.txt"));
    }

    @Test
    //文件下载
    public void testGet() throws IOException {
        fs.copyToLocalFile(false, new Path("hdfs://hadoop102/xiyou/huaguoshan/sunwukong.txt"), new Path("D:\\su"), false);
    }

    //删除
    @Test
    public void testRm() throws IOException {
        //参数解读：
        fs.delete(new Path("/output"), false);
    }

    @Test
    public void testmv() throws IOException {
        fs.rename(new Path("/input/word.txt"), new Path("/input/ss.txt"));

    }

    @Test
    //查看文件详细信息
    public void fileDetail() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        //遍历文件
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("========" + fileStatus.getPath() + "===========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getPath());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            //获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    //判断是文件夹还是文件
    @Test
    public void testFile() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus status : listStatus) {
            if (status.isFile()) {
                System.out.println(status.getPath().getName() + "是一个文件");
            } else {
                System.out.println(status.getPath().getName() + "是一个文件夹");
            }
        }
    }

}



