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
final class GridFileBucket {
    private MongoDatabase database;
    private ReadConcern readConcern;
    private WriteConcern writeConcern;
    private String bucketName;
    private int chunkSize;

    GridFileBucket(MongoDatabase database, ReadConcern readConcern, WriteConcern writeConcern, String bucketName, int chunkSize) {
        this.database = database;
        this.readConcern = readConcern;
        this.writeConcern = writeConcern;
        this.bucketName = bucketName;
        this.chunkSize = chunkSize;
    }

    GridFile get(ObjectId id) {
        GridFileGet action = new GridFileGet(
                getFilesCollection(database, bucketName).withReadConcern(readConcern)
        );
        return action.exec(id);
    }

    GridFile get0(String md5) {
        GridFileGet0 action = new GridFileGet0(
                getFilesCollection(database, bucketName).withReadConcern(readConcern)
        );
        return action.exec(md5);
    }

    void delete(ObjectId id) {
        GridFileDelete action = new GridFileDelete(
                getFilesCollection(database, bucketName).withReadConcern(readConcern).withWriteConcern(writeConcern),
                getChunkCollection(database, bucketName).withReadConcern(readConcern).withWriteConcern(writeConcern)
        );
        action.exec(id);
    }

    void delete0(String md5) {
        GridFileDelete0 action = new GridFileDelete0(
                getFilesCollection(database, bucketName).withReadConcern(readConcern).withWriteConcern(writeConcern),
                getChunkCollection(database, bucketName).withReadConcern(readConcern).withWriteConcern(writeConcern)
        );
        action.exec(md5);
    }

    byte[] read(ObjectId id) {
        GridFileRead action = new GridFileRead(
                getFilesCollection(database, bucketName).withReadConcern(readConcern),
                getChunkCollection(database, bucketName).withReadConcern(readConcern)
        );

        return action.exec(id);
    }

    void read0(ObjectId id, OutputStream out) {
        GridFileRead0 action = new GridFileRead0(
                getFilesCollection(database, bucketName).withReadConcern(readConcern),
                getChunkCollection(database, bucketName).withReadConcern(readConcern)
        );

        action.exec(id, out);
    }

    byte[] readRandom(ObjectId id, long offset, long size) {
        GridFileReadRandom action = new GridFileReadRandom(
                getFilesCollection(database, bucketName).withReadConcern(readConcern),
                getChunkCollection(database, bucketName).withReadConcern(readConcern)
        );

        return action.exec(id, offset, size);
    }

    void readRandom0(ObjectId id, long offset, long size, OutputStream out) {
        GridFileReadRandom0 action = new GridFileReadRandom0(
                getFilesCollection(database, bucketName).withReadConcern(readConcern),
                getChunkCollection(database, bucketName).withReadConcern(readConcern)
        );

        action.exec(id, offset, size, out);
    }

    ObjectId write(String name, String md5, long size, Document metadata, byte[] data) {
        ObjectId id = new ObjectId();
        GridFileWrite action = new GridFileWrite(
                getFilesCollection(database, bucketName).withWriteConcern(writeConcern),
                getChunkCollection(database, bucketName).withWriteConcern(writeConcern),
                chunkSize
        );
        action.exec(id, name, md5, size, metadata, data);
        return id;
    }

    ObjectId write0(String name, String md5, long size, Document metadata, InputStream data) {
        ObjectId id = new ObjectId();
        GridFileWrite0 action = new GridFileWrite0(
                getFilesCollection(database, bucketName).withWriteConcern(writeConcern),
                getChunkCollection(database, bucketName).withWriteConcern(writeConcern),
                chunkSize
        );
        action.exec(id, name, md5, size, metadata, data);
        return id;
    }

    void writeRandom(ObjectId id, long offset, byte[] data) {
        GridFileWriteRandom action = new GridFileWriteRandom(
                getFilesCollection(database, bucketName).withReadConcern(readConcern).withWriteConcern(writeConcern),
                getChunkCollection(database, bucketName).withReadConcern(readConcern).withWriteConcern(writeConcern)
        );
        action.exec(id, offset, data);
    }

    void writeRandom0(ObjectId id, long offset, InputStream data) {
        GridFileWriteRandom0 action = new GridFileWriteRandom0(
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
