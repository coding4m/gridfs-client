package gridfs.client;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

/**
 * @author siuming
 */
final class GridFileRead {
    private final MongoCollection<Document> filesCollection;
    private final MongoCollection<Document> chunkCollection;

    GridFileRead(MongoCollection<Document> filesCollection, MongoCollection<Document> chunkCollection) {
        this.filesCollection = filesCollection;
        this.chunkCollection = chunkCollection;
    }

    byte[] exec(ObjectId id) {
        Document gridFile = filesCollection.find(new Document("_id", id)).first();
        if (null == gridFile) {
            throw new GridFsException(String.format("file[id=%s] not found.", id));
        }

        long fileSize = gridFile.get("length", Number.class).longValue();
        if (fileSize > Integer.MAX_VALUE) {
            throw new GridFsException(String.format("file[id=%s] too large.", id));
        }

        byte[] fileContent = new byte[(int) fileSize];

        int chunkSize = gridFile.get("chunkSize", Number.class).intValue();
        int batchSize = fileSize % chunkSize == 0 ? (int) fileSize / chunkSize : (int) ((fileSize / chunkSize) + 1);

        int writeNr = 0;
        int writeBytes = 0;
        for (Document fileChunk : chunkCollection.find(new Document("files_id", id)).sort(new Document("n", 1)).batchSize(batchSize)) {
            int n = fileChunk.getInteger("n");
            if (n != writeNr) {
                throw new GridFsException(String.format("file[id=%s] not ready.", id));
            }

            byte[] chunk = fileChunk.get("data", Binary.class).getData();
            System.arraycopy(chunk, 0, fileContent, writeBytes, chunk.length);
            writeNr++;
            writeBytes = writeBytes + chunk.length;
        }

        if (writeBytes != fileSize) {
            throw new GridFsException(String.format("file[id=%s] not ready.", id));
        }

        return fileContent;
    }
}
