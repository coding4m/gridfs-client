package gridfs.client;

import org.bson.types.ObjectId;

import java.util.Map;

/**
 * @author siuming
 */
public class GridFile {

    private ObjectId id;
    private long size;
    private long chunkSize;
    private long updateDate;
    private String md5;
    private String filename;
    private Map<String, Object> metadata;

    GridFile() {
    }

    GridFile(Builder builder) {
        this.id = builder.id;
        this.size = builder.size;
        this.chunkSize = builder.chunkSize;
        this.updateDate = builder.updateDate;
        this.md5 = builder.md5;
        this.filename = builder.filename;
        this.metadata = builder.metadata;
    }

    public ObjectId id() {
        return id;
    }

    public long size() {
        return size;
    }

    public long chunkSize() {
        return chunkSize;
    }

    public long updateDate() {
        return updateDate;
    }

    public String md5() {
        return md5;
    }

    public String filename() {
        return filename;
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return "GridFile{" +
                "id=" + id +
                ", size=" + size +
                ", chunkSize=" + chunkSize +
                ", updateDate=" + updateDate +
                ", md5='" + md5 + '\'' +
                ", filename='" + filename + '\'' +
                ", metadata=" + metadata +
                '}';
    }

    /**
     * @return
     */
    static Builder newBuilder() {
        return new Builder();
    }

    static class Builder {
        Builder() {
        }

        ObjectId id;
        long size;
        long chunkSize;
        long updateDate;
        String md5;
        String filename;
        Map<String, Object> metadata;

        public Builder id(ObjectId id) {
            this.id = id;
            return this;
        }

        public Builder size(long size) {
            this.size = size;
            return this;
        }

        public Builder chunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public Builder updateDate(long updateDate) {
            this.updateDate = updateDate;
            return this;
        }

        public Builder md5(String md5) {
            this.md5 = md5;
            return this;
        }

        public Builder filename(String filename) {
            this.filename = filename;
            return this;
        }

        public Builder metadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        public GridFile build() {
            return new GridFile(this);
        }
    }
}
