package com.companyname.apps.entity;

import java.util.HashMap;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.FloatingPointPrecision;

public class ImpalaSchema {

    public enum ImpalaDataType {
        SMALLINT,
        INT,
        STRING,
        FLOAT,
        DOUBLE,
        DATETIME;

        public ArrowType arrowType() {
            switch(this) {
                case SMALLINT: return new ArrowType.Int(16, true);
                case INT: return new ArrowType.Int(32, true);
                case STRING: return new ArrowType.Binary();
                case FLOAT: return new ArrowType.FloatingPoint(FloatingPointPrecision.HALF);
                case DOUBLE: return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
                case DATETIME: return new ArrowType.Timestamp(TimeUnit.MILLISECOND,"Asia/Tokyo");
            }
            throw new RuntimeException("Case not implemented");
        }

    }

    public HashMap<String, ImpalaDataType>cols_type = new HashMap<>();

}
