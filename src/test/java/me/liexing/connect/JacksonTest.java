package me.liexing.connect;

import com.alibaba.rocketmq.client.exception.MQClientException;
import me.liexing.connect.mqconsumer.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jinsheng on 2016/9/22.
 */
public class JacksonTest {

    public static Logger logger = LogManager.getLogger(JacksonTest.class);

    @JsonProperty("consumer")
    static Consumer consumer;

    @JsonProperty("keymap")
    static Map<String, String> keymap;

    public void setConsumer(Consumer consumer) {
        JacksonTest.consumer = consumer;
    }

    public void setKeymap(Map<String, String> keymap) {
        JacksonTest.keymap = keymap;
    }


    @Test
    public void testJackson() throws IOException, MQClientException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);

        JacksonTest jacksonTest  = objectMapper.readValue(JacksonTest.class.getResourceAsStream("/property.json"), JacksonTest.class);
        logger.debug(consumer.getClass().getMethods().toString());

        logger.debug(keymap.toString());
    }

    @Test
    public void testMap() {
        Map<String, String> map = new HashMap<>();
        map.put("", "123");
        map.put(null, "456");
        logger.debug(map);
    }
}
