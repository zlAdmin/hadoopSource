package com.zl.hadoop.hdfs;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * Hadoop HDFS Java API 操作
 *
 * @author zhagnlei
 * @ProjectName: hadoopResource
 * @create 2018-12-02 20:13
 * @Version: 1.0
 * <p>Copyright: Copyright (zl) 2018</p>
 **/
@Slf4j
public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://192.168.1.4:8020";
    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Test
    public void mkDir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    @Test
    public void copyFromLocalFileWithProgress() throws Exception {
        Path localPath = new Path("D:\\BaiduYunDownload/67.浅绿色简洁简历.doc");
        Path hdfsPath = new Path("/hdfsapi/test");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
        InputStream in = new BufferedInputStream(
                new FileInputStream(new File("D:\\BaiduYunDownload/67.浅绿色简洁简历.doc"))
        );
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hsfsapi/test/c.doc"), new Progressable() {
            @Override
            public void progress() {
                //带进度条显示
                System.out.println(".");
            }
        });
        IOUtils.copyBytes(in, outputStream, 4096);
    }

    /**
     * 下载HDFS文件
     * @throws Exception
     */
    @Test
    public void copyToLocalFile() throws Exception {
        Path localPath = new Path("D:\\BaiduYunDownload");
        Path hdfsPath = new Path("/test.txt");
        fileSystem.copyToLocalFile(hdfsPath, localPath);
    }

    /**
     * 递归删除文件
     * @throws Exception
     */
    @Test
    public void deleted() throws Exception {
        fileSystem.delete(new Path("/test"), true);
    }

    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
        log.info("HADOOP setUp");
    }

    @After
    public void tearDown() throws Exception {
        configuration = null;
        fileSystem = null;
        log.info("HADOOP tearDown");
    }
}
