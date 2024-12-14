package main.java.rainStorm;


/**
 * Store basic info a stream tuple.
 */
public class StreamTuple {
    public final String key;
    private final String value;
    private final int stage;
    private final String destFile;

    public StreamTuple(String key, String value, int stage, String destFile) {
        this.key = key;
        this.value = value;
        this.stage = stage;
        this.destFile = destFile;
    }

    public String getKey() { return key; }

    public String getValue() { return value; }

    public int getStage() { return stage; }

    public String getDestFile() { return destFile; }
}
