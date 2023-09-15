package com.indigententerprises.components.objects;

import com.indigententerprises.services.common.SystemException;

import com.indigententerprises.domain.objects.Handle;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 *
 * @author jonniesavell
 */
public class ObjectService {

    private final String SUFFIX = "__METADATA";
    private final com.indigententerprises.services.objects.ObjectService primitiveObjectService;
    private final com.indigententerprises.services.objects.MetaDataService primitiveMetaDataService;

    public ObjectService(
            final com.indigententerprises.services.objects.ObjectService primitiveObjectService,
            final com.indigententerprises.services.objects.MetaDataService primitiveMetaDataService
    ) {
        this.primitiveObjectService = primitiveObjectService;
        this.primitiveMetaDataService = primitiveMetaDataService;
    }

    public Map<String, Object> retrieveObjectMetaData(final Handle handle)
            throws NoSuchElementException, SystemException {

        try {
            final File file = File.createTempFile("temp", ".tmp");

            try {
                final FileOutputStream fileOutputStream = new FileOutputStream(file);

                try {
                    final String identifier = constructMetaDataIdentifier(handle.identifier);
                    this.primitiveObjectService.retrieveObject(identifier, fileOutputStream);
                } finally {
                    fileOutputStream.close();
                }

                final FileInputStream fileInputStream = new FileInputStream(file);

                try {
                    return this.primitiveMetaDataService.deserializeMetaData(fileInputStream);
                } finally {
                    fileInputStream.close();
                }
            } finally {
                if (! file.delete()) {
                    file.deleteOnExit();
                }
            }
        } catch (IOException e) {
            throw new SystemException("", e);
        }
    }

    public void retrieveObject(
            final Handle handle,
            final OutputStream outputStream)
            throws NoSuchElementException, SystemException {

        this.primitiveObjectService.retrieveObject(handle.identifier, outputStream);
    }

    public Map<String, Object> retrieveObjectAndMetaData(
            final Handle handle,
            final OutputStream outputStream)
            throws NoSuchElementException, SystemException {
        try {
            final File file = File.createTempFile("temp", ".tmp");

            try {
                final FileOutputStream fileOutputStream = new FileOutputStream(file);

                try {
                    final String identifier = constructMetaDataIdentifier(handle.identifier);
                    this.primitiveObjectService.retrieveObject(identifier, fileOutputStream);
                } finally {
                    fileOutputStream.close();
                }

                final Map<String, Object> result;
                final FileInputStream fileInputStream = new FileInputStream(file);

                try {
                    result =
                            this.primitiveMetaDataService.deserializeMetaData(fileInputStream);
                    this.primitiveObjectService.retrieveObject(handle.identifier, outputStream);

                    return result;
                } finally {
                    fileInputStream.close();
                }
            } finally {
                if (! file.delete()) {
                    file.deleteOnExit();
                }
            }
        } catch (IOException e) {
            throw new SystemException("", e);
        }
    }

    public Handle storeObjectAndMetaData(
            final InputStream inputStream,
            final int fileSize,
            final Map<String, Object> metadata)
            throws SystemException {
        final UUID uuid = UUID.randomUUID();
        final Handle result = new Handle(uuid.toString());

        this.primitiveObjectService.persistObject(result.identifier, fileSize, inputStream);

        try {
            final File file = File.createTempFile("temp", ".tmp");

            try {
                final FileOutputStream fileOutputStream = new FileOutputStream(file);

                try {
                    this.primitiveMetaDataService.serializeMetaData(fileOutputStream, metadata);
                } finally {
                    fileOutputStream.close();
                }

                final FileInputStream fileInputStream = new FileInputStream(file);

                try {
                    final String identifier = constructMetaDataIdentifier(result.identifier);
                    this.primitiveObjectService.persistObject(identifier, (int) file.length(), fileInputStream);

                    return result;
                } finally {
                    fileInputStream.close();
                }
            } finally {
                if (! file.delete()) {
                    file.deleteOnExit();
                }
            }
        } catch (IOException e) {
            throw new SystemException("", e);
        }
    }

    private String constructMetaDataIdentifier(final String identifier) {
        return identifier + SUFFIX;
    }
}
