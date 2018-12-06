
package com.companyname.apps.controller;

import com.companyname.apps.entity.FilesInsertToImpalaRequestEntity;
import com.companyname.apps.entity.BlobInsertToHdfsResponseEntity;
import com.companyname.apps.util.HDFSFileWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;


import java.io.IOException;
import java.util.logging.Logger;

@RestController
public class FilesInsertToImpalaController {

    //Save the uploaded file to this folder
    private static String UPLOADED_FOLDER = "/root/insert";

    private static final Logger logger = Logger.getLogger(FilesInsertToImpalaController.class.getName());
    static final org.slf4j.Logger LOG = LoggerFactory.getLogger(UserGroupInformation.class);

    @PostMapping(value = "/files/insert/hdfs", consumes = {"multipart/form-data"})
    public ResponseEntity<BlobInsertToHdfsResponseEntity> upload(@RequestParam("file") MultipartFile file,
                                                                 @RequestParam("params") String params,
                                                                 RedirectAttributes redirectAttributes) {

        if (file.isEmpty()) {
            // exception
        }

        logger.info("params: " + params);

        try {

//            BlobInsertToHdfsRequestEntity entity = params;
//            ObjectMapper mapper = new ObjectMapper();
//            FilesInsertToImpalaRequestEntity entity = mapper.readValue(params, FilesInsertToImpalaRequestEntity.class);

            String filename = file.getOriginalFilename();
//            entity.estimateFileTypeIfNeeds(filename);

//            logger.info("to_file_type => " + entity.to_format.file_type.toString());
//            System.out.println("from_file_type!! => " + entity.from_format.file_type.toString());
//            System.out.println("compression_type!! => " + entity.to_format.compression_type.toString());
//            System.out.println("tablename!! => " + entity.destination.tablename.toString());
//            logger.info("schema => " + entity.schema.cols_type.toString());

//            byte[] bytes;
//            if (!entity.needsConvertFormat()) {
////                bytes = file.getBytes();
//            }

            HDFSFileWriter writer = new HDFSFileWriter(false);

            byte[] fileBytes = file.getBytes();
//            byte[] fileBytes2 = Snappy.compress(fileBytes);
//            logger.info("compression: " + entity.to_format.compression_type);
//            if (entity.to_format.compression_type == FileFormatTypes.CompressionType.snappy) {
//                fileBytes = Snappy.compress(fileBytes);
//                logger.info("snappy解凍 => " + new String(Snappy.uncompress(fileBytes)));
//            }

//            logger.info("fileBytes: " + fileBytes.toString());
            writer.write(fileBytes, file.getOriginalFilename(),"/user/hive/warehouse/devdb_api_test.db/test_csv");

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