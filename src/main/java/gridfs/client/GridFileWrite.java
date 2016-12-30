package gridfs.client;

import com.mongodb.client.MongoCollection;
import org.bson.BsonDateTime;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

/**
 * @author siuming
 */
final class GridFileWrite {

    private final MongoCollection<Document> filesCollection;
    private final MongoCollection<Document> chunkCollection;
    private final int chunkSize;

    GridFileWrite(MongoCollection<Document> filesCollection, MongoCollection<Document> chunkCollection, int chunkSize) {
        this.filesCollection = filesCollection;
        this.chunkCollection = chunkCollection;
        this.chunkSize = chunkSize;
    }

    void exec(ObjectId id, String name, String md5, long size, Document metadata, byte[] data) {
        if (data.length > size) {
            throw new GridFsException(String.format("file overflow[size=%s, data=%s].", size, data.length));
        }

        writeChunks(id, data);
        writeFile(id, name, md5, size, metadata);
    }

    private void writeFile(ObjectId id, String name, String md5, long size, Document metadata) {
        Document fileDocument = new Document("_id", id)
                .append("length", size)
                .append("chunkSize", chunkSize)
                .append("uploadDate", new BsonDateTime(System.currentTimeMillis()))
                .append("md5", md5)
                .append("filename", name);
        if (null != metadata && !metadata.isEmpty()) {
            fileDocument.append("metadata", metadata);
        }
        filesCollection.insertOne(fileDocument);
    }

    private void writeChunks(ObjectId id, byte[] data) {
        if (data.length == 0) {
            return;
        }

        int chunkNr = (data.length % chunkSize == 0) ? data.length / chunkSize : (data.length / chunkSize) + 1;
        for (int i = 0; i < chunkNr - 1; i++) {
            byte[] chunk = new byte[chunkSize];
            System.arraycopy(data, i * chunkSize, chunk, 0, chunkSize);
            writeChunk(id, i, chunk);
        }

        int offset = (chunkNr - 1) * chunkSize;
        int remain = data.length - offset;
        byte[] lastChunk = new byte[remain];
        System.arraycopy(data, offset, lastChunk, 0, remain);
        writeChunk(id, chunkNr - 1, lastChunk);
    }

    private void writeChunk(ObjectId id, int n, byte[] chunk) {
        chunkCollection.insertOne(new Document("files_id", id).append("n", n).append("data", new Binary(chunk)));
    }
}
