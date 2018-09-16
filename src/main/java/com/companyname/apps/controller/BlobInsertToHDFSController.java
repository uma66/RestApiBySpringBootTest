package com.companyname.apps.controller;

import com.companyname.apps.entity.BlobInsertToHdfsRequestEntity;
import com.companyname.apps.entity.BlobInsertToHdfsResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.http.HttpHeaders;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.ResponseEntity;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.File;
import java.util.logging.Logger;

@RestController
public class BlobInsertToHDFSController {

    //Save the uploaded file to this folder
    private static final String UPLOADED_FOLDER = "/root/insert";

    private static final Logger logger = Logger.getLogger(BlobInsertToHDFSController.class.getName());

    @PostMapping(value = "/blob/insert/hdfs", consumes = {"multipart/form-data"})
    public ResponseEntity<BlobInsertToHdfsResponseEntity> upload(@RequestParam("file") MultipartFile file,
                                                                 @RequestParam("params") String params,
                                                                 RedirectAttributes redirectAttributes) {

        if (file.isEmpty()) {
            // exception
        }

//        System.out.println("params: " + params.dest_path);
        logger.info("params: " + params);

        try {

//            BlobInsertToHdfsRequestEntity entity = params;
            final ObjectMapper mapper = new ObjectMapper();
            final BlobInsertToHdfsRequestEntity entity = mapper.readValue(params, BlobInsertToHdfsRequestEntity.class);

            logger.info("dest_path!! => " + entity.dest_path);

            final byte[] bytes = file.getBytes();
            final String name = file.getOriginalFilename();

            final Path path = Paths.get(entity.dest_path, name);
            logger.info("path_str: " + path.toString());

            final File uploadFile = new File(Paths.get(entity.dest_path, name).toString());
            final BufferedOutputStream uploadFileStream = new BufferedOutputStream(new FileOutputStream(uploadFile));
            uploadFileStream.write(bytes);
            uploadFileStream.close();

            redirectAttributes.addFlashAttribute("message",
                    "You successfully uploaded '" + file.getOriginalFilename() + "'");

        } catch (IOException e) {
            e.printStackTrace();
        }

        final BlobInsertToHdfsResponseEntity body = new BlobInsertToHdfsResponseEntity();
        body.result = "OK";

        final HttpHeaders headers = new HttpHeaders();
//        headers.add("Responded", "MyController");
        return ResponseEntity.ok().headers(headers).body(body);
//        return new ResponseEntity<>(body, HttpStatus.NOT_FOUND);

    }
}
