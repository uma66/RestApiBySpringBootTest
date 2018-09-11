package com.companyname.apps.util;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.net.URI;
import java.nio.file.Paths;
import java.util.Optional;

public class HDFSFileWriter {

    static private String NameNodeHost = "hdfs://<Namenode-Host>:<Port>";
    private Optional<FileSystem> fileSystem = Optional.ofNullable(null);
    private Configuration conf;
    private Boolean isLocal;

    public static void main(String[] args) {
        String csvStr = "a,b,c\n1,2,3\n4,5,6";
        HDFSFileWriter writer = new HDFSFileWriter(true);
        writer.write(csvStr.getBytes(),"test_2.csv", "/Users/uma6/IdeaProjects/test_api");
    }

    public HDFSFileWriter(Boolean isLocal) {
        this.isLocal = isLocal;
        setConf();
    }

    private void setConf() {
        this.conf = new Configuration();
        if (!this.isLocal) {
            conf.set("fs.defaultFS", NameNodeHost);
        }

        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");
    }

    // Get the filesystem - HDFS or Local
    private FileSystem getFileSystem() {
        if (this.fileSystem.isPresent()) {
            return this.fileSystem.get();
        }
        try {
            String host = isLocal ? "file://localhost" : NameNodeHost;
            this.fileSystem = Optional.of(FileSystem.get(URI.create(host), this.conf));
        } catch (Exception e) {
            System.out.println("e " + e.toString());
        }
        return this.fileSystem.get();
    }

    public void write(byte[] fileBytes, String fileName, String targetDir) {

        try {
            FileSystem fs = getFileSystem();

            //==== Create folder if not exists
            Path targetPath = new Path(targetDir);
            if (!fs.exists(targetPath)) {
                // Create new Directory
                fs.mkdirs(targetPath);
            }

            Path hdfswritepath = new Path(Paths.get(targetDir, "test_2.csv").toString());
            FSDataOutputStream outputStream = fs.create(hdfswritepath);
//            outputStream.writeBytes(csvStr);
            outputStream.write(fileBytes);
            outputStream.close();

        } catch (Exception e) {

            System.out.println("e2: " + e);

        }

    }

}
