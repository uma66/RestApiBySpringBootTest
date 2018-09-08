package com.companyname.apps.util;

import com.companyname.apps.entity.ImpalaSchema;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.column.ColumnDescriptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class HDFSParquetWriter {

    private MessageType parquetSchema;

    static public void makeArrowSchema(HashMap<String, ImpalaSchema.ImpalaDataType> colsType) throws Exception {

//        final List<Field> fieldList = new ArrayList<Field>();
//        colsType.forEach((k, v) -> fieldList += new Field(k, FieldType.nullable(v.arrowType())));

        // java8
        List<Field> fieldList = colsType.entrySet().stream()
                .map(p -> new Field(p.getKey(), FieldType.nullable(p.getValue().arrowType()), null))
                .collect(Collectors.toList());

        Schema schema = new Schema(fieldList, null);
        System.out.println("Arrow Schema is " + schema.toString());

    }

}
