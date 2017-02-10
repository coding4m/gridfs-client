package gridfs.client;

import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author siuming
 */
public class GridFsClient implements Closeable {

    private MongoDatabase database;
    private ReadConcern readConcern;
    private WriteConcern writeConcern;

    private String bucketName;
    private int chunkSize;

    private ExecutorService executorService;

    GridFsClient(Builder builder) {
        this.database = builder.database;
        this.readConcern = builder.readConcern;
        this.writeConcern = builder.writeConcern;
        this.bucketName = builder.bucketName;
        this.chunkSize = builder.chunkSize;
        this.executorService = Executors.newFixedThreadPool(builder.maxThreads);
    }

    public CompletableFuture<GridFile> get(ObjectId id) {
        return CompletableFuture
                .supplyAsync(() -> new GridFileBucket(database, readConcern, writeConcern, bucketName, chunkSize), executorService)
                .thenApplyAsync(bucket -> bucket.get(id), executorService);
    }

    public CompletableFuture<GridFile> get(String md5) {
        return CompletableFuture
                .supplyAsync(() -> new GridFileBucket(database, readConcern, writeConcern, bucketName, chunkSize), executorService)
                .thenApplyAsync(bucket -> bucket.get0(md5), executorService);
    }

    public CompletableFuture<Void> delete(ObjectId id) {
        return CompletableFuture
                .supplyAsync(() -> new GridFileBucket(database, readConcern, writeConcern, bucketName, chunkSize), executorService)
                .thenApplyAsync(bucket -> {
                    bucket.delete(id);
                    return null;
                }, executorService);
    }

    public CompletableFuture<Void> delete(String md5) {
        return CompletableFuture
                .supplyAsync(() -> new GridFileBucket(database, readConcern, writeConcern, bucketName, chunkSize), executorService)
                .thenApplyAsync(bucket -> {
                    bucket.delete0(md5);
                    return null;
                }, executorService);
    }

    public CompletableFuture<byte[]> download(ObjectId id) {
        return CompletableFuture
                .supplyAsync(() -> new GridFileBucket(database, readConcern, writeConcern, bucketName, chunkSize), executorService)
                .thenApplyAsync(bucket -> bucket.read(id), executorService);
    }

    public CompletableFuture<byte[]> download(ObjectId id, long offset, long size) {
        return CompletableFuture
                .supplyAsync(() -> new GridFileBucket(database, readConcern, writeConcern, bucketName, chunkSize), executorService)
                .thenApplyAsync(bucket -> bucket.readR(id, offset, size), executorService);
    }

    public CompletableFuture<Void> download(ObjectId id, OutputStream out) {
        return CompletableFuture
                .supplyAsync(() -> new GridFileBucket(database, readConcern, writeConcern, bucketName, chunkSize), executorService)
                .thenApplyAsync(bucket -> {
                    bucket.read0(id, out);
                    return null;
                }, executorService);
    }

    public CompletableFuture<Void> download(ObjectId id, long offset, long size, OutputStream out) {
        return CompletableFuture
                .supplyAsync(() -> new GridFileBucket(database, readConcern, writeConcern, bucketName, chunkSize), executorService)
                .thenApplyAsync(bucket -> {
                    bucket.readR0(id, offset, size, out);
                    return null;
                }, executorService);
    }

    public CompletableFuture<ObjectId> upload(String filename, String md5, long size, Map<String, Object> metadata, byte[] content) {
        return CompletableFuture
                .supplyAsync(() -> new GridFileBucket(database, readConcern, writeConcern, bucketName, chunkSize), executorService)
                .thenApplyAsync(bucket -> bucket.write(filename, md5, size, new Document(metadata), content), executorService);
    }

    public CompletableFuture<ObjectId> upload(String filename, String md5, long size, Map<String, Object> metadata, InputStream content) {
        return CompletableFuture
                .supplyAsync(() -> new GridFileBucket(database, readConcern, writeConcern, bucketName, chunkSize), executorService)
                .thenApplyAsync(bucket -> bucket.write0(filename, md5, size, new Document(metadata), content), executorService);
    }

    public CompletableFuture<Void> modify(ObjectId id, long offset, byte[] content) {
        return CompletableFuture
                .supplyAsync(() -> new GridFileBucket(database, readConcern, writeConcern, bucketName, chunkSize), executorService)
                .thenApplyAsync(bucket -> {
                    bucket.writeR(id, offset, content);
                    return null;
                }, executorService);
    }

    public CompletableFuture<Void> modify(ObjectId id, long offset, InputStream content) {
        return CompletableFuture
                .supplyAsync(() -> new GridFileBucket(database, readConcern, writeConcern, bucketName, chunkSize), executorService)
                .thenApplyAsync(bucket -> {
                    bucket.writeR0(id, offset, content);
                    return null;
                }, executorService);
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
    }

    /**
     * @return
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        MongoDatabase database;
        ReadConcern readConcern = ReadConcern.DEFAULT;
        WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;

        String bucketName = "fs";
        int chunkSize = 255 * 1024;
        int maxThreads = 50;

        Builder() {
        }

        public Builder database(MongoDatabase database) {
            this.database = database;
            return this;
        }

        public Builder readConcern(ReadConcern readConcern) {
            this.readConcern = readConcern;
            return this;
        }

        public Builder writeConcern(WriteConcern writeConcern) {
            this.writeConcern = writeConcern;
            return this;
        }

        public Builder bucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public Builder chunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public Builder maxThreads(int maxThreads) {
            this.maxThreads = maxThreads;
            return this;
        }

        public GridFsClient build() {
            return new GridFsClient(this);
        }
    }
}
