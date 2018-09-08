package com.companyname.apps.controller;

import com.companyname.apps.entity.BlobInsertHdfsRequestEntity;
import com.companyname.apps.entity.BlobInsertHdfsResponseEntity;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.http.HttpHeaders;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.ResponseEntity;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.File;


@RestController
public class BlobInsertToHDFSController {

    //Save the uploaded file to this folder
    private static String UPLOADED_FOLDER = "/root/insert";

    private Boolean doCheck(String any) {
        return true;
    }

    @PostMapping(value = "/blob/insert/hdfs", consumes = {"multipart/form-data"})
    public ResponseEntity<BlobInsertHdfsResponseEntity> upload(@RequestParam("file") MultipartFile file,
                                                               @RequestParam("params") String params,
                                                               RedirectAttributes redirectAttributes) {

        if (file.isEmpty()) {
            // exception
        }

//        System.out.println("params: " + params.dest_path);
        System.out.println("params: " + params);

        try {
//            BlobInsertHdfsRequestEntity entity = params;
            ObjectMapper mapper = new ObjectMapper();
            BlobInsertHdfsRequestEntity entity = mapper.readValue(params, BlobInsertHdfsRequestEntity.class);

            System.out.println("dest_path!! => " + entity.dest_path);

            byte[] bytes = file.getBytes();
            String name = file.getOriginalFilename();

            Path path = Paths.get(entity.dest_path, name);
            System.out.println("path_str: " + path.toString());

            File uploadFile = new File(Paths.get(entity.dest_path, name).toString());
            BufferedOutputStream uploadFileStream = new BufferedOutputStream(new FileOutputStream(uploadFile));
            uploadFileStream.write(bytes);
            uploadFileStream.close();

            redirectAttributes.addFlashAttribute("message",
                    "You successfully uploaded '" + file.getOriginalFilename() + "'");

        } catch (IOException e) {
            e.printStackTrace();
        }

        BlobInsertHdfsResponseEntity body = new BlobInsertHdfsResponseEntity();
        body.result = "OK";

        HttpHeaders headers = new HttpHeaders();
//        headers.add("Responded", "MyController");
        return ResponseEntity.ok().headers(headers).body(body);
//        return new ResponseEntity<>(body, HttpStatus.NOT_FOUND);

    }
}
