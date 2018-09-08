package com.companyname.apps.entity;

public class FileFormatTypes {

    public enum FileType {
        csv, parquet
    }

    public enum CompressionType {
        none, gzip, snappy
    }

    public CompressionType compression_type;
    public FileType file_type;

}
