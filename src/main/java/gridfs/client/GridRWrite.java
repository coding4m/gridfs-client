package gridfs.client;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

/**
 * @author siuming
 */
final class GridRWrite {

    private final MongoCollection<Document> filesCollection;
    private final MongoCollection<Document> chunkCollection;

    GridRWrite(MongoCollection<Document> filesCollection, MongoCollection<Document> chunkCollection) {
        this.filesCollection = filesCollection;
        this.chunkCollection = chunkCollection;
    }

    void exec(ObjectId id, long offset, byte[] data) {
        if (data.length <= 0) {
            return;
        }

        Document gridFile = filesCollection.find(new Document("_id", id)).first();
        if (null == gridFile) {
            throw new GridFsException(String.format("file[id=%s] not found.", id));
        }

        long fileSize = gridFile.get("length", Number.class).longValue();
        if (fileSize < offset + data.length) {
            throw new GridFsException(String.format("file[id=%s] offset out of range, size=%s", id, fileSize));
        }
        int chunkSize = gridFile.get("chunkSize", Number.class).intValue();
        int lastChunkSize = (int) fileSize % chunkSize;

        int fileChunkNr = (int) (fileSize % chunkSize == 0 ? fileSize / chunkSize : (fileSize / chunkSize) + 1);
        int lastChunkNr = fileChunkNr - 1;

        int firstWriteNr = (int) (offset / chunkSize);
        int firstWriteSize = (int) ((offset + data.length) >= chunkSize ? chunkSize - (offset % chunkSize) : data.length);
        int firstWriteOffset = (int) (offset % chunkSize);
        if (firstWriteNr == lastChunkNr) {
            writeLastChunk(id, firstWriteNr, firstWriteOffset, firstWriteSize, data, 0, lastChunkSize);
            return;
        }

        writePartialChunk(id, firstWriteNr, firstWriteOffset, firstWriteSize, data, 0, chunkSize);
        int lastWriteNr = (int) ((offset + data.length) / chunkSize);
        if (firstWriteNr == lastWriteNr) {
            return;
        }

        int lastWriteSize = (int) ((offset + data.length) % chunkSize);
        if (lastWriteNr == lastChunkNr) {
            writeLastChunk(id, lastWriteNr, 0, lastWriteSize, data, firstWriteSize + ((lastWriteNr - 1) * chunkSize), lastChunkSize);
        } else {
            writePartialChunk(id, lastWriteNr, 0, lastWriteSize, data, firstWriteSize + ((lastWriteNr - 1) * chunkSize), chunkSize);
        }

        for (int i = firstWriteNr + 1; i < lastWriteNr; i++) {
            writeChunk(id, i, data, firstWriteSize + ((i - 1) * chunkSize), chunkSize);
        }
    }

    private void writeChunk(ObjectId id, int nr, byte[] data, int dataOffset, int chunkSize) {
        byte[] buf = new byte[chunkSize];
        System.arraycopy(data, dataOffset, buf, 0, chunkSize);
        chunkCollection.updateOne(
                Filters.and(Filters.eq("files_id", id), Filters.eq("n", nr)),
                new Document("$set", new Document("files_id", id).append("n", nr).append("data", new Binary(buf))),
                new UpdateOptions().upsert(true)
        );
    }

    private void writePartialChunk(ObjectId id, int nr, int writeOffset, int writeSize, byte[] data, int dataOffset, int chunkSize) {
        Document chunk = chunkCollection.find(new Document("files_id", id).append("n", nr)).first();
        byte[] chunkData = null != chunk ? chunk.get("data", Binary.class).getData() : new byte[chunkSize];
        System.arraycopy(data, dataOffset, chunkData, writeOffset, writeSize);
        chunkCollection.updateOne(
                Filters.and(Filters.eq("files_id", id), Filters.eq("n", nr)),
                new Document("$set", new Document("files_id", id).append("n", nr).append("data", new Binary(chunkData))),
                new UpdateOptions().upsert(true)
        );
    }

    private void writeLastChunk(ObjectId id, int nr, int writeOffset, int writeSize, byte[] data, int dataOffset, int chunkSize) {
        Document chunk = chunkCollection.find(new Document("files_id", id).append("n", nr)).first();
        byte[] chunkData = null != chunk ? chunk.get("data", Binary.class).getData() : new byte[chunkSize];
        System.arraycopy(data, dataOffset, chunkData, writeOffset, writeSize);
        chunkCollection.updateOne(
                Filters.and(Filters.eq("files_id", id), Filters.eq("n", nr)),
                new Document("$set", new Document("files_id", id).append("n", nr).append("data", new Binary(chunkData))),
                new UpdateOptions().upsert(true)
        );
    }
}
