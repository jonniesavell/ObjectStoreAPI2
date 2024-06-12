package com.indigententerprises.components;

import com.indigententerprises.components.files.FileInvestigativeServiceImplementation;
import com.indigententerprises.components.objects.MetaDataServiceImplementation;
import com.indigententerprises.components.objects.ObjectService;
import com.indigententerprises.components.objects.ObjectServiceImplementation;
import com.indigententerprises.components.streams.TrivialStreamTransferService;

import com.indigententerprises.services.common.SystemException;
import com.indigententerprises.services.files.FileInvestigativeService;
import com.indigententerprises.services.objects.IObjectService;
import com.indigententerprises.services.streams.StreamTransferService;

import software.amazon.awssdk.services.s3.S3Client;

/**
 * @author jonniesavell
 */
public class ObjectStorageComponent {

    private final ObjectService objectService;
    private final FileInvestigativeService fileInvestigativeService;

    public ObjectStorageComponent(
            final S3Client s3Client,
            final String requestedBucketName
    ) throws SystemException {
        final String targetBucketName = requestedBucketName;
        this.fileInvestigativeService = new FileInvestigativeServiceImplementation();
        final StreamTransferService streamTransferService = new TrivialStreamTransferService();
        final com.indigententerprises.services.objects.ObjectService primitiveObjectService =
                new ObjectServiceImplementation(
                        s3Client,
                        targetBucketName,
                        streamTransferService
                );
        final com.indigententerprises.services.objects.MetaDataService primitiveMetaDataService =
                new MetaDataServiceImplementation();
        this.objectService = new ObjectService(primitiveObjectService, primitiveMetaDataService);
    }

    public IObjectService getObjectService() {
        return objectService;
    }

    public FileInvestigativeService getFileInvestigativeService() {
        return fileInvestigativeService;
    }
}
