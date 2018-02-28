package gridfs.client;

import com.mongodb.MongoClient;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.InputStream;
import java.io.OutputStream;

import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * @author siuming
 */
final class GridBucket {
    private MongoDatabase database;
    private ReadConcern readConcern;
    private WriteConcern writeConcern;
    private String bucketName;
    private int chunkSize;

    GridBucket(MongoDatabase database, ReadConcern readConcern, WriteConcern writeConcern, String bucketName, int chunkSize) {
        this.database = database;
        this.readConcern = readConcern;
        this.writeConcern = writeConcern;
        this.bucketName = bucketName;
        this.chunkSize = chunkSize;
    }

    GridFile get(ObjectId id) {
        GridGet action = new GridGet(
                getFilesCollection(database, bucketName).withReadConcern(readConcern)
        );
        return action.exec(id);
    }

    GridFile get0(String md5) {
        GridGet0 action = new GridGet0(
                getFilesCollection(database, bucketName).withReadConcern(readConcern)
        );
        return action.exec(md5);
    }

    void delete(ObjectId id) {
        GridDelete action = new GridDelete(
                getFilesCollection(database, bucketName).withReadConcern(readConcern).withWriteConcern(writeConcern),
                getChunkCollection(database, bucketName).withReadConcern(readConcern).withWriteConcern(writeConcern)
        );
        action.exec(id);
    }

    void delete0(String md5) {
        GridDelete0 action = new GridDelete0(
                getFilesCollection(database, bucketName).withReadConcern(readConcern).withWriteConcern(writeConcern),
                getChunkCollection(database, bucketName).withReadConcern(readConcern).withWriteConcern(writeConcern)
        );
        action.exec(md5);
    }

    byte[] read(ObjectId id) {
        GridRead action = new GridRead(
                getFilesCollection(database, bucketName).withReadConcern(readConcern),
                getChunkCollection(database, bucketName).withReadConcern(readConcern)
        );

        return action.exec(id);
    }

    void read0(ObjectId id, OutputStream out) {
        GridRead0 action = new GridRead0(
                getFilesCollection(database, bucketName).withReadConcern(readConcern),
                getChunkCollection(database, bucketName).withReadConcern(readConcern)
        );

        action.exec(id, out);
    }

    byte[] readR(ObjectId id, long offset, long size) {
        GridRRead action = new GridRRead(
                getFilesCollection(database, bucketName).withReadConcern(readConcern),
                getChunkCollection(database, bucketName).withReadConcern(readConcern)
        );

        return action.exec(id, offset, size);
    }

    void readR0(ObjectId id, long offset, long size, OutputStream out) {
        GridRRead0 action = new GridRRead0(
                getFilesCollection(database, bucketName).withReadConcern(readConcern),
                getChunkCollection(database, bucketName).withReadConcern(readConcern)
        );

        action.exec(id, offset, size, out);
    }

    ObjectId write(String name, String md5, long size, Document metadata, byte[] data) {
        ObjectId id = new ObjectId();
        GridWrite action = new GridWrite(
                getFilesCollection(database, bucketName).withWriteConcern(writeConcern),
                getChunkCollection(database, bucketName).withWriteConcern(writeConcern),
                chunkSize
        );
        action.exec(id, name, md5, size, metadata, data);
        return id;
    }

    ObjectId write0(String name, String md5, long size, Document metadata, InputStream data) {
        ObjectId id = new ObjectId();
        GridWrite0 action = new GridWrite0(
                getFilesCollection(database, bucketName).withWriteConcern(writeConcern),
                getChunkCollection(database, bucketName).withWriteConcern(writeConcern),
                chunkSize
        );
        action.exec(id, name, md5, size, metadata, data);
        return id;
    }

    void writeR(ObjectId id, long offset, byte[] data) {
        GridRWrite action = new GridRWrite(
                getFilesCollection(database, bucketName).withReadConcern(readConcern).withWriteConcern(writeConcern),
                getChunkCollection(database, bucketName).withReadConcern(readConcern).withWriteConcern(writeConcern)
        );
        action.exec(id, offset, data);
    }

    void writeR0(ObjectId id, long offset, InputStream data) {
        GridRWrite0 action = new GridRWrite0(
                getFilesCollection(database, bucketName).withReadConcern(readConcern).withWriteConcern(writeConcern),
                getChunkCollection(database, bucketName).withReadConcern(readConcern).withWriteConcern(writeConcern)
        );
        action.exec(id, offset, data);
    }


    private static MongoCollection<Document> getFilesCollection(final MongoDatabase database, final String bucketName) {
        return database
                .getCollection(bucketName + ".files")
                .withCodecRegistry(fromRegistries(database.getCodecRegistry(), MongoClient.getDefaultCodecRegistry()));
    }

    private static MongoCollection<Document> getChunkCollection(final MongoDatabase database, final String bucketName) {
        return database
                .getCollection(bucketName + ".chunks")
                .withCodecRegistry(MongoClient.getDefaultCodecRegistry());
    }
}
