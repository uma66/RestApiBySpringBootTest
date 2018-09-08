package com.companyname.apps.entity;

import org.apache.commons.io.FilenameUtils;
import com.companyname.apps.entity.FileFormatTypes.*;
public class FilesInsertToImpalaRequestEntity {

    public TableInfo destination;
    public FileFormatTypes from_format = new FileFormatTypes();
    public FileFormatTypes to_format = new FileFormatTypes();
    public ImpalaSchema schema = new ImpalaSchema();

    public Boolean needsConvertFormat() {
        // toもfromも判明しており、かつfromとtoが違う場合だけ変換が必要
        return from_format.file_type != FileType.unknown & to_format.file_type != FileType.unknown & from_format.file_type != to_format.file_type;
    }

    public void estimateFileTypeIfNeeds(String filename) {
        if (from_format.file_type == FileType.unknown) {
            from_format.file_type = FileType.valueOf(FilenameUtils.getExtension(filename));
            System.out.println("拡張子からファイル種別を判定 => " + from_format.file_type.toString());
        }
    }

}
