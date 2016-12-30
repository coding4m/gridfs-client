package gridfs.client;

/**
 * @author siuming
 */
public class GridFsException extends RuntimeException{
    public GridFsException() {
    }

    public GridFsException(String message) {
        super(message);
    }

    public GridFsException(String message, Throwable cause) {
        super(message, cause);
    }

    public GridFsException(Throwable cause) {
        super(cause);
    }
}
