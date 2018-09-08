package com.companyname.apps.entity;

public class FileFormatTypes {

    public enum FileType {
        unknown,
        csv,
        parquet;
    }

    // これだとMappingしてくれない。
//    public enum FileType {
//        UNKNOWN("unknown"),
//        CSV("csv"),
//        PARQUET("parquet");
//
//        private final String type;
//        FileType(String type) {
//            this.type = type;
//        }
//        public String getValue() { return type; }
//    }

    public enum CompressionType {
        none, gzip, snappy
    }

    public CompressionType compression_type = CompressionType.none;
    public FileType file_type = FileType.unknown;

}
