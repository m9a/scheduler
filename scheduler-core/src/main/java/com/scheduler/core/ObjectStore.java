package com.scheduler.core;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * Thin wrapper around S3Client for MinIO object storage.
 * Used by both coordinator (upload input files, serve output downloads)
 * and worker (download inputs, upload outputs and logs).
 */
public class ObjectStore {

    private final S3Client s3;
    private final String bucket;

    public ObjectStore(S3Client s3, String bucket) {
        this.s3 = Objects.requireNonNull(s3);
        this.bucket = Objects.requireNonNull(bucket);
    }

    public void putObject(String key, byte[] content) {
        s3.putObject(
                PutObjectRequest.builder().bucket(bucket).key(key).build(),
                RequestBody.fromBytes(content));
    }

    public void putObject(String key, Path file) {
        s3.putObject(
                PutObjectRequest.builder().bucket(bucket).key(key).build(),
                file);
    }

    public void getObject(String key, Path destination) {
        s3.getObject(
                GetObjectRequest.builder().bucket(bucket).key(key).build(),
                destination);
    }

    public InputStream getObjectStream(String key) {
        return s3.getObject(
                GetObjectRequest.builder().bucket(bucket).key(key).build());
    }

    public long getObjectSize(String key) {
        HeadObjectResponse head = s3.headObject(
                HeadObjectRequest.builder().bucket(bucket).key(key).build());
        return head.contentLength();
    }

    public List<ObjectInfo> listObjects(String prefix) {
        ListObjectsV2Response response = s3.listObjectsV2(
                ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build());
        return response.contents().stream()
                .map(obj -> new ObjectInfo(obj.key(), obj.size()))
                .toList();
    }

    public boolean exists(String key) {
        try {
            s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    public record ObjectInfo(String key, long sizeBytes) {}
}
