package gridfs.client;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import java.io.InputStream;

/**
 * @author siuming
 */
final class GridFileWriteRandom0 {

    private final MongoCollection<Document> filesCollection;
    private final MongoCollection<Document> chunkCollection;

    GridFileWriteRandom0(MongoCollection<Document> filesCollection, MongoCollection<Document> chunkCollection) {
        this.filesCollection = filesCollection;
        this.chunkCollection = chunkCollection;
    }

    void exec(ObjectId id, long offset, InputStream data) {
        Document gridFile = filesCollection.find(new Document("_id", id)).first();
        if (null == gridFile) {
            throw new GridFsException(String.format("file[id=%s] not found.", id));
        }

        long fileSize = gridFile.get("length", Number.class).longValue();
        int chunkSize = gridFile.get("chunkSize", Number.class).intValue();
        int lastChunkSize = (int) fileSize % chunkSize;

        int fileChunkNr = (int) (fileSize % chunkSize == 0 ? fileSize / chunkSize : (fileSize / chunkSize) + 1);
        int lastChunkNr = fileChunkNr - 1;

        int firstWriteNr = (int) (offset / chunkSize);
        int firstWriteOffset = (int) (offset % chunkSize);

        int writeNr = firstWriteNr;
        long writeBytes = 0;
        for (; ; ) {

            ChunkRead write;
            if (writeNr == firstWriteNr) {
                write = readChunk(data, chunkSize - firstWriteOffset);
            } else if (writeNr == lastChunkNr) {
                write = readChunk(data, lastChunkSize);
            } else {
                write = readChunk(data, chunkSize);
            }

            if (write.readSize == -1) {
                break;
            }

            byte[] readBytes = write.readBytes;
            int readSize = write.readSize;

            writeBytes = writeBytes + readSize;
            if (writeBytes + offset > fileSize) {
                throw new GridFsException(String.format("file[id=%s] offset out of range, size=%s", id, fileSize));
            }

            int writeOffset = writeNr == firstWriteNr ? firstWriteOffset : 0;
            if (writeNr == lastChunkNr) {
                writeLastChunk(id, writeNr, writeOffset, readSize, readBytes, lastChunkSize);
                break;
            }

            if (writeNr == firstWriteNr) {
                writePartialChunk(id, writeNr, writeOffset, readSize, readBytes, chunkSize);
                writeNr++;
                continue;
            }

            writeChunk(id, writeNr, readSize, readBytes, chunkSize);
            writeNr++;
        }
    }

    private ChunkRead readChunk(InputStream data, int chunkSize) {
        try {
            byte[] writeBytes = new byte[chunkSize];
            int writeSize = data.read(writeBytes);
            return new ChunkRead(writeBytes, writeSize);
        } catch (Exception e) {
            throw new GridFsException(e);
        }
    }

    private void writeChunk(ObjectId id, int nr, int writeSize, byte[] data, int chunkSize) {
        byte[] buf = new byte[chunkSize];
        System.arraycopy(data, 0, buf, 0, writeSize);
        chunkCollection.updateOne(
                new Document("files_id", id).append("n", nr),
                new Document("$set", new Document("files_id", id).append("n", nr).append("data", new Binary(buf))),
                new UpdateOptions().upsert(true)
        );
    }

    private void writePartialChunk(ObjectId id, int nr, int writeOffset, int writeSize, byte[] data, int chunkSize) {
        Document chunk = chunkCollection.find(new Document("files_id", id).append("n", nr)).first();
        byte[] chunkData = null != chunk ? chunk.get("data", Binary.class).getData() : new byte[chunkSize];
        System.arraycopy(data, 0, chunkData, writeOffset, writeSize);
        chunkCollection.updateOne(
                new Document("files_id", id).append("n", nr),
                new Document("$set", new Document("files_id", id).append("n", nr).append("data", new Binary(chunkData))),
                new UpdateOptions().upsert(true)
        );
    }

    private void writeLastChunk(ObjectId id, int nr, int writeOffset, int writeSize, byte[] data, int chunkBytes) {
        Document chunk = chunkCollection.find(new Document("files_id", id).append("n", nr)).first();
        byte[] chunkData = null != chunk ? chunk.get("data", Binary.class).getData() : new byte[chunkBytes];
        System.arraycopy(data, 0, chunkData, writeOffset, writeSize);
        chunkCollection.updateOne(
                new Document("files_id", id).append("n", nr),
                new Document("$set", new Document("files_id", id).append("n", nr).append("data", new Binary(chunkData))),
                new UpdateOptions().upsert(true)
        );
    }

    private static class ChunkRead {
        final byte[] readBytes;
        final int readSize;

        private ChunkRead(byte[] readBytes, int readSize) {
            this.readBytes = readBytes;
            this.readSize = readSize;
        }
    }
}
