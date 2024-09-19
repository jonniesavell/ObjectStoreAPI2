package com.indigententerprises.factories;

import com.indigententerprises.components.ObjectStorageComponent;
import com.indigententerprises.services.common.SystemException;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
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
        final AwsCredentials credentials = credentialsProvider.resolveCredentials();

        System.out.println("access-key-id" + credentials.accessKeyId());

        final S3Client s3Client = S3Client.builder()
                .credentialsProvider(credentialsProvider)
                .build();
        final ObjectStorageComponent result = new ObjectStorageComponent(s3Client, bucketName);
        return result;
    }
}
