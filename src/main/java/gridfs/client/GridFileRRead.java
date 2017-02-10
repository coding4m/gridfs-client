package gridfs.client;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

/**
 * @author siuming
 */
final class GridFileRRead {
    private final MongoCollection<Document> filesCollection;
    private final MongoCollection<Document> chunkCollection;

    GridFileRRead(MongoCollection<Document> filesCollection, MongoCollection<Document> chunkCollection) {
        this.filesCollection = filesCollection;
        this.chunkCollection = chunkCollection;
    }

    byte[] exec(ObjectId id, long offset, long size) {
        if (size == 0) {
            return new byte[0];
        }

        Document gridFile = filesCollection.find(new Document("_id", id)).first();
        if (null == gridFile) {
            throw new GridFsException(String.format("file[id=%s] not found.", id));
        }

        long fileSize = gridFile.get("length", Number.class).longValue();
        if (fileSize < offset + size) {
            throw new GridFsException(String.format("file[id=%s] offset out of range, size=%s", id, fileSize));
        }
        if (size > Integer.MAX_VALUE) {
            throw new GridFsException(String.format("size=%s too large.", size));
        }

        byte[] fileContent = new byte[(int) size];

        int chunkSize = gridFile.get("chunkSize", Number.class).intValue();
        int firstReadNr = (int) (offset / chunkSize);
        int firstReadOffset = (int) (offset % chunkSize);
        int firstReadSize = (int) (size > chunkSize ? chunkSize - firstReadOffset : size - firstReadOffset);
        int lastReadNr = (int) ((offset + size) / chunkSize);
        int lastReadSize = (int) ((offset + size) % chunkSize);

        int writeNr = firstReadNr;
        int writeBytes = 0;
        int batchSize = lastReadNr - firstReadNr + 1;

        Document filter = new Document("files_id", id).append("n", new Document("$gte", firstReadNr).append("$lte", lastReadNr));
        for (Document fileChunk : chunkCollection.find(filter).sort(new Document("n", 1)).batchSize(batchSize)) {
            int n = fileChunk.getInteger("n");
            if (n != writeNr) {
                throw new GridFsException(String.format("file[id=%s] not ready.", id));
            }

            byte[] chunk = fileChunk.get("data", Binary.class).getData();
            if (writeNr == firstReadNr) {
                System.arraycopy(chunk, firstReadOffset, fileContent, writeBytes, firstReadSize);
                writeNr++;
                writeBytes = writeBytes + firstReadSize;
                continue;
            }

            if (writeNr == lastReadNr) {
                System.arraycopy(chunk, 0, fileContent, writeBytes, lastReadSize);
                writeNr++;
                writeBytes = writeBytes + lastReadSize;
                continue;
            }

            System.arraycopy(chunk, 0, fileContent, writeBytes, chunk.length);
            writeNr++;
            writeBytes = writeBytes + chunk.length;
        }

        if (writeBytes != size) {
            throw new GridFsException(String.format("file[id=%s] not ready.", id));
        }
        return fileContent;
    }
}
