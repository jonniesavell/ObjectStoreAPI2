package com.indigententerprises.components.objects;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;

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

/**
 * @author jonniesavell
 */
public class ObjectServiceImplementation implements
        com.indigententerprises.services.objects.ObjectService {
    private final S3Client s3Client;
    private final String targetBucketName;
    private final StreamTransferService streamTransferService;

    public ObjectServiceImplementation(
            final S3Client s3Client,
            final String targetBucketName,
            final StreamTransferService streamTransferService
    ) {
        this.s3Client = s3Client;
        this.targetBucketName = targetBucketName;
        this.streamTransferService = streamTransferService;
    }

    @Override
    public void persistObject(
            final String id,
            final int size,
            final InputStream sourceInputStream
    ) throws SystemException {
        try {
            final PutObjectRequest putObjectRequest =
                    PutObjectRequest.builder()
                           .bucket(targetBucketName)
                           .key(id)
                           .metadata(Collections.emptyMap())
                           .build();
            s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(sourceInputStream, size));
        } catch (NoSuchBucketException e) {
            throw new SystemException("bucket " + this.targetBucketName + " not found", e);
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
            final HeadBucketRequest headBucketRequest =
                    HeadBucketRequest
                            .builder()
                            .bucket(targetBucketName)
                            .build();
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
            throw new NoSuchElementException();
        } catch (IOException e) {
            throw new SystemException("", e);
        } catch (AwsServiceException e) {
            throw new SystemException("", e);
        } catch (SdkException e) {
            throw new SystemException("", e);
        }
    }
}
