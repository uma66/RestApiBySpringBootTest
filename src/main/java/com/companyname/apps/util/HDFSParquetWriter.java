package com.companyname.apps.util;

import com.companyname.apps.controller.HDFSWritableByteChannel;
import com.companyname.apps.entity.ImpalaSchema;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
//import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileOutputStream;

import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;


public class HDFSParquetWriter {

    private MessageType parquetSchema;
    private Schema arrowSchema;
    private Configuration conf;
    private RootAllocator ra = null;
    private Path arrowPath;
    private VectorSchemaRoot arrowVectorSchemaRoot;
    private ArrowFileWriter arrowFileWriter;
    private HashMap<String, ImpalaSchema.ImpalaDataType> colsType;

    public HDFSParquetWriter(Schema arrowSchema) {
        this.conf = new Configuration();
        this.ra = new RootAllocator(Integer.MAX_VALUE);
        this.arrowSchema = arrowSchema;
//        this.colsType = colsType;
    }

    public void write() {

        String outputPath = "/Users/uma6/IdeaProjects/test_api";
        Boolean overwrite = true;
        CompressionCodecName codec = CompressionCodecName.valueOf("snappy");

        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://<Namenode-Host>:<Port>");
//
//        try {
//
//            Path path = new Path(outputPath);
//
//            try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
//                    .<GenericData.Record>builder(path)
//                    .withWriterVersion(PARQUET_2_0)
//                    .withWriteMode(overwrite ?
//                            ParquetFileWriter.Mode.OVERWRITE : ParquetFileWriter.Mode.CREATE)
//                    .withCompressionCodec(codec)
//                    .withDictionaryEncoding(true)
//                    .withDictionaryPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
//                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
//                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
//                    .withDataModel(GenericData.get())
//                    .withConf(conf)
//                    .withSchema(this.arrowSchema)
//                    .build()) {
//
////                for (GenericData.Record record : reader) {
////                    writer.write(record);
////                }
//
//            } catch (RuntimeException e) {
//                throw new RuntimeException("Failed on record ", e);
//            }

//        } catch (IllegalArgumentException e) {
//            System.out.println("illegal: " + e.toString());
//        }

    }

    private void makeArrowSchema() throws Exception {
        // java8
        List<Field> fieldList = this.colsType.entrySet().stream()
                .map(p -> new Field(p.getKey(), FieldType.nullable(p.getValue().arrowType()), null))
                .collect(Collectors.toList());

        this.arrowSchema = new Schema(fieldList, null);
        System.out.println("Arrow Schema is " + this.arrowSchema.toString());
    }

    private void setArrowFileWriter(String arrowFileName) throws Exception {
        String arrowFullPath = this.arrowPath.toUri().toString() + "/" + arrowFileName;
        System.out.println("Creating a file with name : " + arrowFullPath);
        // create the file stream on HDFS
        Path path = new Path(arrowFullPath);
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        // default is to over-write
        FSDataOutputStream file = fs.create(new Path(path.toUri().getRawPath()));
        this.arrowVectorSchemaRoot = VectorSchemaRoot.create(this.arrowSchema, this.ra);
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();

        boolean writeLocalFile = false;
        if (writeLocalFile) {
            File arrowFile = new File("./" + arrowFileName);
            FileOutputStream fileOutputStream = new FileOutputStream(arrowFile);
            this.arrowFileWriter = new ArrowFileWriter(this.arrowVectorSchemaRoot,
                    provider,
                    fileOutputStream.getChannel());
        } else {
            /* use HDFS files */
            this.arrowFileWriter = new ArrowFileWriter(this.arrowVectorSchemaRoot,
                    provider,
                    new HDFSWritableByteChannel(file));
        }
    }

}
