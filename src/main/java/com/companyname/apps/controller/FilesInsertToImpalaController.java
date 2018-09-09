package com.companyname.apps.controller;

import com.companyname.apps.entity.FileFormatTypes;
import com.companyname.apps.entity.FilesInsertToImpalaRequestEntity;
import com.companyname.apps.entity.BlobInsertToHdfsResponseEntity;
import com.companyname.apps.util.HDFSParquetWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;


import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;


@RestController
public class FilesInsertToImpalaController {

    //Save the uploaded file to this folder
    private static String UPLOADED_FOLDER = "/root/insert";

    private Boolean doCheck(String any) {
        return true;
    }

    @PostMapping(value = "/files/insert/hdfs", consumes = {"multipart/form-data"})
    public ResponseEntity<BlobInsertToHdfsResponseEntity> upload(@RequestParam("file") MultipartFile file,
                                                                 @RequestParam("params") String params,
                                                                 RedirectAttributes redirectAttributes) {

        if (file.isEmpty()) {
            // exception
        }

        System.out.println("params: " + params);

        try {

//            BlobInsertToHdfsRequestEntity entity = params;
            ObjectMapper mapper = new ObjectMapper();
            FilesInsertToImpalaRequestEntity entity = mapper.readValue(params, FilesInsertToImpalaRequestEntity.class);

            String filename = file.getOriginalFilename();
            entity.estimateFileTypeIfNeeds(filename);

            System.out.println("to_file_type!! => " + entity.to_format.file_type.toString());
//            System.out.println("from_file_type!! => " + entity.from_format.file_type.toString());
//            System.out.println("compression_type!! => " + entity.to_format.compression_type.toString());
//            System.out.println("tablename!! => " + entity.destination.tablename.toString());
            System.out.println("schema!! => " + entity.schema.cols_type.toString());

            try {
                HDFSParquetWriter writer = new HDFSParquetWriter(entity.schema.makeArrowSchema());
            } catch (Exception e) {
                System.out.println("error!!! " + e.toString());
            }

            byte[] bytes;

            if (!entity.needsConvertFormat()) {
                bytes = file.getBytes();
            }

//            Path path = Paths.get(entity.dest_path, name);
//            System.out.println("path_str: " + path.toString());
//
//            File uploadFile = new File(Paths.get(entity.dest_path, name).toString());
//            BufferedOutputStream uploadFileStream = new BufferedOutputStream(new FileOutputStream(uploadFile));
//            uploadFileStream.write(bytes);
//            uploadFileStream.close();

            redirectAttributes.addFlashAttribute("message",
                    "You successfully uploaded '" + file.getOriginalFilename() + "'");

        } catch (IOException e) {
            e.printStackTrace();
            BlobInsertToHdfsResponseEntity res = new BlobInsertToHdfsResponseEntity();
            res.msg = e.toString();
            return ResponseEntity.badRequest().body(res);
        }

        BlobInsertToHdfsResponseEntity body = new BlobInsertToHdfsResponseEntity();
        body.result = "OK";

        HttpHeaders headers = new HttpHeaders();
//        headers.add("Responded", "MyController");
        return ResponseEntity.ok().headers(headers).body(body);
//        return new ResponseEntity<>(body, HttpStatus.NOT_FOUND);
    }

}
