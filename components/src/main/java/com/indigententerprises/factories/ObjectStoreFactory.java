package com.indigententerprises.factories;

import com.indigententerprises.components.ObjectStorageComponent;
import com.indigententerprises.services.common.SystemException;

import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * @author jonniesavell
 */
public class ObjectStoreFactory {

    public static ObjectStorageComponent createObjectStorageComponent(
            final String bucketName
    ) throws SystemException {
        final EnvironmentVariableCredentialsProvider credentialsProvider =
                EnvironmentVariableCredentialsProvider.create();
        final S3Client s3Client = S3Client.builder()
                .credentialsProvider(credentialsProvider)
                .build();
//        final HeadBucketRequest headBucketRequest =
//                HeadBucketRequest
//                        .builder()
//                        .bucket(targetBucketName)
//                        .build();
//        try {
//            s3Client.headBucket(headBucketRequest);
//        } catch (NoSuchBucketException e) {
//            throw new RuntimeException("bucket " + this.targetBucketName + " not found");
//        }

        final ObjectStorageComponent result = new ObjectStorageComponent(s3Client, bucketName);
        return result;
    }
}
