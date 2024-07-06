package com.indigententerprises.applications;

import com.indigententerprises.components.ObjectStorageComponent;

import com.indigententerprises.factories.ObjectStoreFactory;

import com.indigententerprises.services.common.SystemException;
import com.indigententerprises.services.files.FileInvestigativeService;
import com.indigententerprises.services.objects.IObjectService;

import com.indigententerprises.domain.files.FileData;
import com.indigententerprises.domain.objects.Handle;
import com.indigententerprises.domain.objects.HandleAndArnPair;

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
                    final String targetBucketName = requestedBucketName;
                    final ObjectStorageComponent objectStorageComponent =
                            ObjectStoreFactory.createObjectStorageComponent(targetBucketName);
                    final IObjectService objectService = objectStorageComponent.getObjectService();
                    final FileInvestigativeService fileInvestigativeService =
                            objectStorageComponent.getFileInvestigativeService();
                    final FileData fileData = fileInvestigativeService.investigate(file);
                    final HandleAndArnPair handleAndArnPair =
                            objectService.storeObjectAndMetaData(
                                    fileData.getInputStream(),
                                    (int) fileData.getFileMetaData().getSize(),
                                    new HashMap<>()
                            );
                    final Handle handle = handleAndArnPair.handle;

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
