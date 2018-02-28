package gridfs.client;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.Collections;
import java.util.Date;

/**
 * @author siuming
 */
final class GridGet0 {
    private final MongoCollection<Document> filesCollection;

    GridGet0(MongoCollection<Document> filesCollection) {
        this.filesCollection = filesCollection;
    }

    GridFile exec(String md5) {
        Document gridFile = filesCollection.find(new Document("md5", md5)).first();
        if (null == gridFile) {
            return null;
        }

        Document metadata = gridFile.get("metadata", Document.class);
        return GridFile.newBuilder()
                .id(gridFile.getObjectId("_id"))
                .size(gridFile.get("length", Number.class).longValue())
                .chunkSize(gridFile.get("chunkSize", Number.class).intValue())
                .updateDate(gridFile.get("uploadDate", Date.class).getTime())
                .md5(gridFile.getString("md5"))
                .filename(gridFile.getString("filename"))
                .metadata(null == metadata ? Collections.emptyMap() : metadata)
                .build();
    }
}
