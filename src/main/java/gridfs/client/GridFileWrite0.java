package gridfs.client;

import com.mongodb.client.MongoCollection;
import org.bson.BsonDateTime;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import java.io.InputStream;

/**
 * @author siuming
 */
final class GridFileWrite0 {
    private final MongoCollection<Document> filesCollection;
    private final MongoCollection<Document> chunkCollection;
    private final int chunkSize;

    GridFileWrite0(MongoCollection<Document> filesCollection, MongoCollection<Document> chunkCollection, int chunkSize) {
        this.filesCollection = filesCollection;
        this.chunkCollection = chunkCollection;
        this.chunkSize = chunkSize;
    }


    void exec(ObjectId id, String name, String md5, long size, Document metadata, InputStream data) {
        writeChunks(id, data);
        writeFile(id, name, md5,size, metadata);
    }

    private void writeChunks(ObjectId id, InputStream data) {
        int chunkNr = 0;
        int readBytes;
        byte[] buf = new byte[chunkSize];
        for (; ; ) {
            try {
                readBytes = data.read(buf);
                if (readBytes == -1) {
                    break;
                }

                byte[] chunk = buf;
                if (readBytes != chunkSize) {
                    chunk = new byte[readBytes];
                    System.arraycopy(buf, 0, chunk, 0, readBytes);
                }

                writeChunk(id, chunkNr, chunk);
                chunkNr++;
            } catch (Exception e) {
                throw new GridFsException(e);
            }
        }
    }

    private void writeFile(ObjectId id, String name, String md5, long size, Document metadata) {
        Document fileDocument = new Document("_id", id)
                .append("length", size)
                .append("chunkSize", chunkSize)
                .append("uploadDate", new BsonDateTime(System.currentTimeMillis()))
                .append("md5", md5)
                .append("filename", name);
        if (metadata != null && !metadata.isEmpty()) {
            fileDocument.append("metadata", metadata);
        }
        filesCollection.insertOne(fileDocument);
    }

    private void writeChunk(ObjectId id, int n, byte[] chunk) {
        chunkCollection.insertOne(new Document("files_id", id).append("n", n).append("data", new Binary(chunk)));
    }
}
