package com.indigententerprises.components.objects;

import com.indigententerprises.services.common.SystemException;
import com.indigententerprises.services.streams.StreamTransferService;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
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
    public Collection<String> retrieveKeysByPrefix(final String prefix) throws SystemException {
        final Collection<String> result = new LinkedList<>();
        final ListObjectsV2Request request =
                ListObjectsV2Request.builder()
                        .bucket(this.targetBucketName)
                        .prefix(prefix)
                        .build();
        final ListObjectsV2Response response = this.s3Client.listObjectsV2(request);

        for (S3Object object : response.contents()) {
            result.add(object.key());
        }

        return result;
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
            // TODO: kill head-bucket-request: useless round-trip to S3
            final HeadBucketRequest headBucketRequest =
                    HeadBucketRequest
                            .builder()
                            .bucket(this.targetBucketName)
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
        } catch (NoSuchKeyException e) {
            throw new NoSuchElementException();
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

    @Override
    public void deleteObject(final String id) throws NoSuchElementException, SystemException {
        try {
            final DeleteObjectRequest deleteObjectRequest =
                    DeleteObjectRequest
                            .builder()
                            .bucket(this.targetBucketName)
                            .key(id)
                            .build();
            final DeleteObjectResponse deleteObjectResponse =
                    s3Client.deleteObject(deleteObjectRequest);
        } catch (NoSuchKeyException e) {
            throw new NoSuchElementException();
        } catch (NoSuchBucketException e) {
            throw new NoSuchElementException();
        } catch (AwsServiceException e) {
            throw new SystemException("", e);
        } catch (SdkException e) {
            throw new SystemException("", e);
        }
    }
}
