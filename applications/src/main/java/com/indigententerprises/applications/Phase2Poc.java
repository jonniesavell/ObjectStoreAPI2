package com.indigententerprises.applications;

import com.indigententerprises.components.files.FileInvestigativeServiceImplementation;
import com.indigententerprises.components.objects.MetaDataServiceImplementation;
import com.indigententerprises.components.objects.ObjectService;
import com.indigententerprises.components.objects.ObjectServiceImplementation;
import com.indigententerprises.components.streams.TrivialStreamTransferService;

import com.indigententerprises.services.common.SystemException;
import com.indigententerprises.services.files.FileInvestigativeService;
import com.indigententerprises.services.streams.StreamTransferService;

import com.indigententerprises.domain.files.FileData;
import com.indigententerprises.domain.objects.Handle;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class Phase2Poc {

    public static void main(final String [] args) throws SystemException {

        if (args.length != 3) {
            throw new RuntimeException("usage: <exec> " + Phase2Poc.class.getName() + " <AWS bucket-name> <directory> <file>");
        } else {
            final String directoryName = args[1];
            final String fileName = args[2];
            final File directory = new File(directoryName);
            final File file = new File(directory, fileName);

            if (!file.canRead()) {
                throw new RuntimeException("file " + file.getName() + " is not accessible");
            } else {
                final String requestedBucketName = args[0];

                try {
                    final EnvironmentVariableCredentialsProvider credentialsProvider =
                            EnvironmentVariableCredentialsProvider.create();
                    final String targetBucketName = requestedBucketName;
                    final FileInvestigativeService fileInvestigativeService =
                            new FileInvestigativeServiceImplementation();
                    final FileData fileData = fileInvestigativeService.investigate(file);
                    final StreamTransferService streamTransferService = new TrivialStreamTransferService();
                    final com.indigententerprises.services.objects.ObjectService primitiveObjectService =
                            new ObjectServiceImplementation(
                                    credentialsProvider,
                                    targetBucketName,
                                    streamTransferService
                            );
                    final com.indigententerprises.services.objects.MetaDataService primitiveMetaDataService =
                            new MetaDataServiceImplementation();
                    final ObjectService objectService =
                            new ObjectService(primitiveObjectService, primitiveMetaDataService);
                    final Handle handle;

                    handle = objectService.storeObjectAndMetaData(
                            fileData.getInputStream(),
                            (int) fileData.getFileMetaData().getSize(),
                            new HashMap<>());

                    final FileOutputStream fileOutputStream = new FileOutputStream(file, false);

                    // truncate the local file
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

                    final FileOutputStream fileOutputStream2 = new FileOutputStream(file);

                    try {
                        Map<String, Object> metadata =
                                objectService.retrieveObjectAndMetaData(handle, fileOutputStream2);

                        System.out.println(metadata);
                    } finally {
                        fileOutputStream2.close();
                    }
                } catch(IOException e){
                    throw new SystemException("", e);
                } catch(NoSuchElementException e){
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
