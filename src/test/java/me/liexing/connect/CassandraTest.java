package me.liexing.connect;

import com.datastax.driver.core.ColumnMetadata;
import me.liexing.connect.processor.Processor;
import me.liexing.connect.producer.CassandraProducer;
import me.liexing.connect.producer.Producer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jinsheng on 16/9/18.
 */
public class CassandraTest {

    private static Logger logger = LogManager.getLogger(CassandraTest.class);

    @JsonProperty("prodecer")
    static Producer producer;

    public void setProducer(Producer producer) {
        CassandraTest.producer = producer;
    }


    @Before
    public void create() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);

        try {
            CassandraTest cassandraTest = objectMapper.readValue(this.getClass().getResourceAsStream("/property.json"), CassandraTest.class);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    @After
    public void close() {
        producer.close();
    }
    @Test
    public void testGetColumns() {
        List<ColumnMetadata> columns = ((CassandraProducer)producer).getColumns();
        for (ColumnMetadata column:columns) {
            logger.debug(column.getName());
            logger.debug(column.getType());
        }
        ((CassandraProducer)producer).close();
    }

    @Test
    public void jsonInsertTest() throws IOException {
        String testJson = "{\"name\":\"zhangsan\",\"id\":1,\"map1\":{\"key\":\"value\"}}";


        //this.cassandraProducer.pushJsonData(testJson);
    }

    @Test
    public void jacksonTest() throws IOException {
        String testJson1 = "{\"name\":\"zhangsan\",\"id\":1,\"map1\":{\"key\":\"value\"}}";
        String testJson2 = "{\"name\":\"zhangsan\",\"id\":1,\"map1\":{\"map\":{\"key\":\"value\"}}}";
        ObjectMapper objectMapper = new ObjectMapper();
        Map test = objectMapper.readValue(testJson1, Map.class);
        Map test2 = objectMapper.readValue(testJson2, Map.class);
        logger.debug(test);
        logger.debug(test.get("map1").getClass().getName());
        logger.debug(objectMapper.writeValueAsString(test));
        logger.debug(((Map)test.get("map1")).get("key"));


        logger.debug(test2);
        logger.debug(test2.get("map1").getClass().getName());
        logger.debug(objectMapper.writeValueAsString(test2));
        logger.debug(((Map)test2.get("map1")).get("key"));



    }
}
