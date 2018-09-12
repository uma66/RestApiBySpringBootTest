package com.companyname.apps.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.nio.file.Paths;
import java.util.Optional;
import java.util.logging.Logger;


public class HDFSFileWriter {

    static private String NameNodeHost = "hdfs://<Namenode-Host>:<Port>";
    private Optional<FileSystem> fileSystem = Optional.ofNullable(null);
    private Configuration conf;
    private Boolean isLocal;
    private static final Logger logger = Logger.getLogger(HDFSFileWriter.class.getName());

    public static void main(String[] args) {
        final String csvStr = "a,b,c\n1,2,3\n4,5,6";
        final HDFSFileWriter writer = new HDFSFileWriter(true);
        writer.write(csvStr.getBytes(),"test_2.csv", "/Users/uma6/IdeaProjects/test_api");
    }

    public HDFSFileWriter(Boolean isLocal) {
        this.isLocal = isLocal;
        setConf();
        authWithKerberos();
    }

    private void setConf() {
        this.conf = new Configuration();
        if (this.isLocal) {
            this.conf.set("fs.defaultFS", "file://localhost");
        } else {
            this.conf.set("fs.defaultFS", NameNodeHost);
        }

        logger.info("defaultFS=" + this.conf.get("fs.defaultFS"));

        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");
    }

    // TODO: 未テスト.
    private void authWithKerberos() {
        final String user = "username@REALM.COM";
        final String keyPath = "username.keytab";
        try {
            UserGroupInformation.setConfiguration(this.conf);
            UserGroupInformation.loginUserFromKeytab(user, keyPath);
        } catch (Exception e) {
            System.out.println("kerberos error: " + e.toString());
        }
    }

    // Get the filesystem - HDFS or Local
    private FileSystem getFileSystem() {
        if (this.fileSystem.isPresent()) {
            return this.fileSystem.get();
        }
        try {
//            String host = isLocal ? "file://localhost" : NameNodeHost;
//            this.fileSystem = Optional.of(FileSystem.get(URI.create(host), this.conf));
            this.fileSystem = Optional.of(FileSystem.get(this.conf));
        } catch (Exception e) {
            System.out.println("e " + e.toString());
        }
        return this.fileSystem.get();
    }

    public void write(byte[] fileBytes, String fileName, String targetDir) {

        try {
            final FileSystem fs = getFileSystem();

            //==== Create folder if not exists
            final Path targetPath = new Path(targetDir);
            if (!fs.exists(targetPath)) {
                // Create new Directory
                fs.mkdirs(targetPath);
            }

            final Path hdfswritepath = new Path(Paths.get(targetDir, fileName).toString());
            FSDataOutputStream outputStream = fs.create(hdfswritepath);
//            outputStream.writeBytes(csvStr);
            outputStream.write(fileBytes);
            outputStream.close();
            logger.info("書き込み完了 => " + hdfswritepath.toString());

        } catch (Exception e) {

            logger.severe("e2: " + e);

        }

    }

}
