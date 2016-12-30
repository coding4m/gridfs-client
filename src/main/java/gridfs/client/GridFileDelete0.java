package gridfs.client;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;

/**
 * @author siuming
 */
final class GridFileDelete0 {
    private final MongoCollection<Document> filesCollection;
    private final MongoCollection<Document> chunkCollection;

    GridFileDelete0(MongoCollection<Document> filesCollection, MongoCollection<Document> chunkCollection) {
        this.filesCollection = filesCollection;
        this.chunkCollection = chunkCollection;
    }

    void exec(String md5) {
        Document doc = filesCollection.findOneAndDelete(Filters.eq("md5", md5));
        if (null != doc) {
            chunkCollection.deleteMany(Filters.eq("files_id", doc.getObjectId("_id")));
        }
    }
}
