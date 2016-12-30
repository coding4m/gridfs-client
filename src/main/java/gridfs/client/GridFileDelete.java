package gridfs.client;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 * @author siuming
 */
final class GridFileDelete {
    private final MongoCollection<Document> filesCollection;
    private final MongoCollection<Document> chunkCollection;

    GridFileDelete(MongoCollection<Document> filesCollection, MongoCollection<Document> chunkCollection) {
        this.filesCollection = filesCollection;
        this.chunkCollection = chunkCollection;
    }

    void exec(ObjectId id) {
        filesCollection.deleteOne(Filters.eq("_id", id));
        chunkCollection.deleteMany(Filters.eq("files_id", id));
    }
}
