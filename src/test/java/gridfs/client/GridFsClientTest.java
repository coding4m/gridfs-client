package gridfs.client;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;

/**
 * @author siuming
 */
public class GridFsClientTest {

    MongoClient mongo;
    GridFsClient client;

    @Before
    public void setUp() throws Exception {
        mongo = new MongoClient(new MongoClientURI("mongodb://172.16.1.248:27017/workplus"));
        MongoDatabase database = mongo.getDatabase("workplus");
        client = GridFsClient.newBuilder()
                .database(database)
                .build();

    }

    @Test
    public void testGet() throws Exception {
        CompletableFuture<GridFile> result = client.get(new ObjectId("56d641b28314bfddf13bc45c"));
        GridFile gridFile = result.get();
        System.out.println(gridFile);
    }

    @Test
    public void testGet0() throws Exception {
        CompletableFuture<GridFile> result = client.get("8222445d9ffa9ca5d06c1409f5aab0d8");
        GridFile gridFile = result.get();
        System.out.println(gridFile);
    }

    @Test
    public void testRead() throws Exception {
        CompletableFuture<byte[]> result = client.download(new ObjectId("56d641b28314bfddf13bc45c"));
        byte[] content = result.get();
        System.out.println(content.length);
    }

    @Test
    public void testRead0() throws Exception {
        OutputStream out = new FileOutputStream("/tmp/test-gridfs.jpg");
        CompletableFuture<Void> result = client.download(new ObjectId("56d641b28314bfddf13bc45c"), out);
        result.get();
    }

    @Test
    public void testReadRandom() throws Exception {
        CompletableFuture<byte[]> result = client.download(new ObjectId("56d641b28314bfddf13bc45c"), 0, 461218);
        byte[] content = result.get();
        System.out.println(content.length);
    }

    @Test
    public void testReadRandom0() throws Exception {
        OutputStream out = new FileOutputStream("/tmp/test-read-random.jpg");
        CompletableFuture<Void> result = client.download(new ObjectId("56d641b28314bfddf13bc45c"), 0, 461218, out);
        result.get();
    }

    @Test
    public void testWrite() throws Exception {
        CompletableFuture<byte[]> readResult = client.download(new ObjectId("56d641b28314bfddf13bc45c"));
        byte[] content = readResult.get();
        System.out.println(content.length);

        CompletableFuture<ObjectId> writeResult = client.upload("abc.jpg", "", 461218, new Document(), content);
        ObjectId id = writeResult.get();
        System.out.println(id);

        OutputStream out = new FileOutputStream("/tmp/test-read-after-write.jpg");
        CompletableFuture<Void> result = client.download(id, 0, 461218, out);
        result.get();
    }

    @Test
    public void testWrite0() throws Exception {
        InputStream in = new FileInputStream("/tmp/test-read0.jpg");
        CompletableFuture<ObjectId> writeResult = client.upload("abc.jpg", "", 461218, new Document(), in);
        ObjectId id = writeResult.get();
        System.out.println(id);

        OutputStream out = new FileOutputStream("/tmp/test-read0-after-write.jpg");
        CompletableFuture<Void> result = client.download(id, 0, 461218, out);
        result.get();

    }

    @Test
    public void testWriteRandom() throws Exception {
        CompletableFuture<byte[]> readResult = client.download(new ObjectId("56d641b28314bfddf13bc45c"));
        byte[] content = readResult.get();
        System.out.println(content.length);

        CompletableFuture<ObjectId> writeResult = client.upload("abc.jpg", "", 461218, new Document(), new byte[0]);
        ObjectId id = writeResult.get();
        System.out.println(id);

        byte[] firstSegment = new byte[100];
        System.arraycopy(content, 0, firstSegment, 0, firstSegment.length);
        byte[] secondSegment = new byte[1000];
        System.arraycopy(content, 100, secondSegment, 0, secondSegment.length);
        byte[] thirdSegment = new byte[10000];
        System.arraycopy(content, 1000 + 100, thirdSegment, 0, thirdSegment.length);
        byte[] fourSegment = new byte[461218 - 10000 - 1000 - 100];
        System.arraycopy(content, 10000 + 1000 + 100, fourSegment, 0, fourSegment.length);
        CompletableFuture<Void> modifyResult = client.modify(id, 0, firstSegment);
        modifyResult.get();

        modifyResult = client.modify(id, 100, secondSegment);
        modifyResult.get();

        modifyResult = client.modify(id, 10000 + 1000 + 100, fourSegment);
        modifyResult.get();

        modifyResult = client.modify(id, 1000 + 100, thirdSegment);
        modifyResult.get();

        OutputStream out = new FileOutputStream("/tmp/test-random.jpg");
        CompletableFuture<Void> result = client.download(id, out);
        result.get();
    }

    @After
    public void tearDown() throws Exception {
        client.close();
        mongo.close();
    }
}
