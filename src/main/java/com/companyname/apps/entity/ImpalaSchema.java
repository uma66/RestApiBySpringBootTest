package com.companyname.apps.entity;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

public class ImpalaSchema {

    public enum ImpalaDataType {
        SMALLINT,
        INT,
        STRING,
        FLOAT,
        DOUBLE,
        DATETIME;

        public ArrowType arrowType() {
            switch (this) {
                case SMALLINT:
                    return new ArrowType.Int(16, true);
                case INT:
                    return new ArrowType.Int(32, true);
                case STRING:
                    return new ArrowType.Binary();
                case FLOAT:
                    return new ArrowType.FloatingPoint(FloatingPointPrecision.HALF);
                case DOUBLE:
                    return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
                case DATETIME:
                    return new ArrowType.Timestamp(TimeUnit.MILLISECOND, "Asia/Tokyo");
            }
            throw new RuntimeException("Case not implemented");
        }

    }

    public HashMap<String, ImpalaDataType> cols_type = new HashMap<>();

    public Schema makeArrowSchema() throws Exception {
        // java8
        List<Field> fieldList = cols_type.entrySet().stream()
                .map(p -> new Field(p.getKey(), FieldType.nullable(p.getValue().arrowType()), null))
                .collect(Collectors.toList());

        Schema schema = new Schema(fieldList, null);
        System.out.println("Arrow Schema is " + schema.toString());
        return schema;
    }

}
