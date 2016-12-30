package gridfs.client;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import java.io.OutputStream;

/**
 * @author siuming
 */
final class GridFileReadRandom0 {
    private final MongoCollection<Document> filesCollection;
    private final MongoCollection<Document> chunkCollection;

    GridFileReadRandom0(MongoCollection<Document> filesCollection, MongoCollection<Document> chunkCollection) {
        this.filesCollection = filesCollection;
        this.chunkCollection = chunkCollection;
    }

    void exec(ObjectId id, long offset, long size, OutputStream out) {

        if (size == 0) {
            return;
        }

        Document gridFile = filesCollection.find(new Document("_id", id)).first();
        if (null == gridFile) {
            throw new GridFsException(String.format("file[id=%s] not found.", id));
        }

        long fileSize = gridFile.get("length", Number.class).longValue();
        if (fileSize < offset + size) {
            throw new GridFsException(String.format("file[id=%s] offset out of range, size=%s", id, fileSize));
        }

        int chunkSize = gridFile.get("chunkSize", Number.class).intValue();
        int firstReadNr = (int) (offset / chunkSize);
        int firstReadOffset = (int) (offset % chunkSize);
        int firstReadSize = (int) (size > chunkSize ? chunkSize - firstReadOffset : size - firstReadOffset);
        int lastReadNr = (int) ((offset + size) / chunkSize);
        int lastReadSize = (int) ((offset + size) % chunkSize);

        long writeBytes = 0;
        int writeNr = firstReadNr;
        int batchSize = lastReadNr - firstReadNr + 1;

        Document filter = new Document("files_id", id).append("n", new Document("$gte", firstReadNr).append("$lte", lastReadNr));
        for (Document fileChunk : chunkCollection.find(filter).sort(new Document("n", 1)).batchSize(batchSize)) {
            int n = fileChunk.getInteger("n");
            if (n != writeNr) {
                throw new GridFsException(String.format("file[id=%s] not ready.", id));
            }

            byte[] chunk = fileChunk.get("data", Binary.class).getData();
            try {

                if (writeNr == firstReadNr) {
                    out.write(chunk, firstReadOffset, firstReadSize);
                    out.flush();

                    writeNr++;
                    writeBytes = writeBytes + firstReadSize;
                    continue;
                }

                if (writeNr == lastReadNr) {
                    out.write(chunk, 0, lastReadSize);
                    out.flush();

                    writeNr++;
                    writeBytes = writeBytes + lastReadSize;
                    continue;
                }

                out.write(chunk);
                out.flush();

                writeNr++;
                writeBytes = writeBytes + chunk.length;
            } catch (Exception e) {
                throw new GridFsException(e);
            }
        }

        if (writeBytes != size) {
            throw new GridFsException(String.format("file[id=%s] not ready.", id));
        }
    }
}
