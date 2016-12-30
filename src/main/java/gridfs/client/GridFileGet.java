package gridfs.client;

import com.mongodb.client.MongoCollection;
import org.bson.BsonDateTime;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.Collections;
import java.util.Date;

/**
 * @author siuming
 */
final class GridFileGet {
    private final MongoCollection<Document> filesCollection;

    GridFileGet(MongoCollection<Document> filesCollection) {
        this.filesCollection = filesCollection;
    }

    GridFile exec(ObjectId id) {
        Document gridFile = filesCollection.find(new Document("_id", id)).first();
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
