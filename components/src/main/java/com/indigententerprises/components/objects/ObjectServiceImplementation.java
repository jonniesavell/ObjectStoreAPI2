package com.indigententerprises.components.objects;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import com.indigententerprises.services.common.SystemException;
import com.indigententerprises.services.streams.StreamTransferService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.NoSuchElementException;

public class ObjectServiceImplementation implements
        com.indigententerprises.services.objects.ObjectService {

    private final AwsCredentialsProvider credentialsProvider;
    private final String targetBucketName;
    private final StreamTransferService streamTransferService;

    public ObjectServiceImplementation(
            final AwsCredentialsProvider credentialsProvider,
            final String targetBucketName,
            final StreamTransferService streamTransferService) throws SystemException {

        this.credentialsProvider = credentialsProvider;
        this.targetBucketName = targetBucketName;
        this.streamTransferService = streamTransferService;

        try {
            final S3Client s3Client =
                    S3Client
                            .builder()
                            .credentialsProvider(this.credentialsProvider)
                            .build();
            try {
                final HeadBucketRequest headBucketRequest =
                        HeadBucketRequest
                                .builder()
                                .bucket(targetBucketName)
                                .build();
                try {
                    s3Client.headBucket(headBucketRequest);
                    final AwsCredentials credentials = credentialsProvider.resolveCredentials();
                    // ???
                } catch (NoSuchBucketException e) {
                    throw new RuntimeException("bucket " + this.targetBucketName + " not found");
                }
            } finally {
                s3Client.close();
            }
        } catch (AwsServiceException e) {
            throw new SystemException("", e);
        } catch (SdkException e) {
            throw new SystemException("", e);
        }
    }

    @Override
    public void persistObject(
            final String id,
            final int size,
            final InputStream sourceInputStream
    ) throws SystemException {
        try {
            final S3Client s3Client =
                    S3Client
                            .builder()
                            .credentialsProvider(this.credentialsProvider)
                            .build();
            try {
                final HeadBucketRequest headBucketRequest =
                        HeadBucketRequest
                                .builder()
                                .bucket(targetBucketName)
                                .build();
                try {
                    s3Client.headBucket(headBucketRequest);
                    final AwsCredentials credentials = credentialsProvider.resolveCredentials();
                    final PutObjectRequest putObjectRequest =
                            PutObjectRequest.builder()
                                   .bucket(targetBucketName)
                                   .key(id)
                                   .metadata(Collections.emptyMap())
                                   .build();
                    s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(sourceInputStream, size));
                } catch (NoSuchBucketException e) {
                    throw new RuntimeException("bucket " + this.targetBucketName + " not found");
                }
            } finally {
                s3Client.close();
            }
        } catch (AwsServiceException e) {
            throw new SystemException("", e);
        } catch (SdkException e) {
            throw new SystemException("", e);
        }
    }

    @Override
    public void retrieveObject(
            final String id,
            final OutputStream outputStream
    ) throws NoSuchElementException, SystemException {
        try {
            final S3Client s3Client =
                    S3Client
                            .builder()
                            .credentialsProvider(this.credentialsProvider)
                            .build();
            try {
                final HeadBucketRequest headBucketRequest =
                        HeadBucketRequest
                                .builder()
                                .bucket(targetBucketName)
                                .build();
                try {
                    s3Client.headBucket(headBucketRequest);
                    final GetObjectRequest getObjectRequest =
                            GetObjectRequest
                                    .builder()
                                    .bucket(this.targetBucketName)
                                    .key(id)
                                    .build();
                    final ResponseBytes<GetObjectResponse> objectBytes =
                            s3Client.getObjectAsBytes(getObjectRequest);
                    final InputStream inputStream = objectBytes.asInputStream();

                    try {
                        streamTransferService.transferStreamData(inputStream, outputStream);
                    } finally {
                        inputStream.close();
                    }
                } catch (NoSuchBucketException e) {
                    throw new NoSuchElementException(e);
                }
            } finally {
                s3Client.close();
            }
        } catch (IOException e) {
            throw new SystemException("", e);
        } catch (AwsServiceException e) {
            throw new SystemException("", e);
        } catch (SdkException e) {
            throw new SystemException("", e);
        }
    }
}
