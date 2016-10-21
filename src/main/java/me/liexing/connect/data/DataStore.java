package me.liexing.connect.data;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by jinsheng on 16/9/19.
 */
public class DataStore {

    public static BlockingQueue<Object> sourceBlockingQueue = null;

    public static BlockingQueue<Object> producerBlockingQueue = null;

    @JsonCreator
    public DataStore(@JsonProperty("LocalCacheSize") int localCacheSize) {
        sourceBlockingQueue = new ArrayBlockingQueue<Object>(localCacheSize);
        producerBlockingQueue = new ArrayBlockingQueue<Object>(localCacheSize);
    }

}
