package com.indigententerprises.applications;

import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 *
 * test the meta-data service
 *
 * @author jonniesavell
 *
 */
public class ObjectManipulationTest {

    private File file;

    //@Before
    public void before() throws IOException {

        file = new File("object.bin");

        if (file.exists()) {
            final FileOutputStream fileOutputStream = new FileOutputStream(file, false);

            try {
                FileChannel outChannel = fileOutputStream.getChannel();

                try {
                    outChannel.truncate(0);
                } finally {
                    outChannel.close();
                }
            } finally {
                fileOutputStream.close();
            }
        } else {
            // file doesn't exist => no need to do anything
        }

        final FileOutputStream fileOutputStream = new FileOutputStream(file, false);
        int size;

        try {
            byte byteNumber = Byte.MIN_VALUE;
            size = 0;

            while (byteNumber < Byte.MAX_VALUE) {

                fileOutputStream.write(byteNumber);

                // update mutable state
                byteNumber++;
                size++;
            }
        } finally {
            fileOutputStream.close();
        }
    }

    //@After
    public void afterTest() throws IOException {

        if (file.exists()) {
            file.delete();
        }
    }

    //@Test
    public void testObjectStuff() throws S3Exception, java.io.FileNotFoundException, java.io.IOException {

        // this is where YOU enter YOUR credentials.
        final EnvironmentVariableCredentialsProvider credentialsProvider =
                EnvironmentVariableCredentialsProvider.create();
        S3Client s3 = S3Client.builder()
                              .credentialsProvider(credentialsProvider)
                              .build();

        try {
            final Map<String, String> metadata = new HashMap<>();
            metadata.put("x-amz-meta-val", "13");

            final PutObjectRequest putObjectRequest =
                    PutObjectRequest.builder()
                            .bucket("com.indigententerprises.photos")
                            .key("pants")
                            .metadata(metadata)
                            .build();

            s3.putObject(putObjectRequest, RequestBody.fromFile(file));
        } catch (S3Exception e) {
            throw e;
        }

        try {
            final GetObjectRequest getObjectRequest =
                    GetObjectRequest.builder()
                                    .key("pants")
                                    .bucket("com.indigententerprises.photos")
                                    .build();
            final ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(getObjectRequest);
            final byte [] data = objectBytes.asByteArray();
            final File newFile = new File("object2.bin");

            try (FileOutputStream fileOutputStream = new FileOutputStream(newFile)) {
                fileOutputStream.write(data);
            }
        } catch (S3Exception e) {
            throw e;
        }
    }
}
