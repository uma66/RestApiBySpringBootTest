package com.companyname.apps.util;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;


public class HDFSFileWriterTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testWriteCsvToLocal() throws Exception {

        File testFolder = tempFolder.newFolder("testFolder");
        System.out.println("テストフォルダのパス: " + testFolder.getPath());

        assertTrue(testFolder.list().length == 0);

        final String csvStr = "a,b,c\n1,2,3\n4,5,6";
        final HDFSFileWriter writer = new HDFSFileWriter(true);
        writer.write(csvStr.getBytes(), "test.csv", testFolder.getPath());

        assertTrue(testFolder.exists());

        String[] filterFiles = testFolder.list(new FilenameFilter() {
            public boolean accept(File file, String str){
                // 拡張子を指定する
                if (str.endsWith("crc")){
                    return false;
                } else {
                    return true;
                }
            }
        });

        assertEquals("test.csv", filterFiles[0]);

        Path path = Paths.get(testFolder.getPath(),"test.csv");

        try {
            byte[] bytes = Files.readAllBytes(path);
            assertArrayEquals(bytes, csvStr.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
