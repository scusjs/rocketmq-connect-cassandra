package me.liexing.connect.processor;

import me.liexing.connect.data.DataStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by jinsheng on 16/9/19.
 */
public class DataConversionHandler implements Runnable {

    private DataConversion dataConversion;

    private Map<String, String> keymap;

    private static Logger logger = LogManager.getLogger(DataConversionHandler.class);


    @JsonCreator
    public DataConversionHandler(@JsonProperty("SchemaFilename") String schemaFilename, @JsonProperty("keymap") Map keymap) throws IOException {
        this.dataConversion = new AvroConversion(schemaFilename);
        this.keymap = keymap;
    }

    public void run() {
        while(true) {
            try {
                Object source = DataStore.sourceBlockingQueue.take();
                List<Map> lists = this.dataConversion.convert(source);
                for (Map map: lists) {
                    logger.debug(map);
                    if (map != null) {
                        //keymap
                        for (Map.Entry entry : keymap.entrySet()) {
                            if (entry.getValue() != null &&((String)(entry.getValue())).length() > 0 && map.containsKey(entry.getKey()))
                                map.put(entry.getValue(), map.remove(entry.getKey()));
                            else
                                map.remove(entry.getKey());
                        }
                    }
                    logger.debug(map);
                    DataStore.producerBlockingQueue.put(map);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }
        }
    }

    public DataConversion getDataConversion() {
        return this.dataConversion;
    }
}
