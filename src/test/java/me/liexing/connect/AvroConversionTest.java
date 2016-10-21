package me.liexing.connect;

import com.alibaba.rocketmq.common.message.MessageExt;
import me.liexing.connect.processor.AvroConversion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * Created by jinsheng on 16/9/19.
 */
public class AvroConversionTest {

    private AvroConversion avroConversion;

    private static Logger logger = LogManager.getLogger(AvroConversionTest.class);


    @Before
    public void create() throws IOException {
        this.avroConversion = new AvroConversion("/data.avsc");
    }

    @Test
    public void testAvroConvert() throws IOException {
        String testJson = "{\"name1\":\"zhangsan\",\"id\":1,\"map1\":{\"map\":{\"key\":\"value\"}},\"set1\":{\"array\":[\"1\",\"2\"]}}";
        //String testJson = "{\"name\":\"zhangsan\",\"id\":1,\"map1\":{\"key\":\"value\"}}";

        byte[] bytecode = avroConversion.avroencode(testJson);

        logger.debug(bytecode);
        logger.debug(bytecode.length);


        String result = avroConversion.avrodecode(bytecode);

        logger.debug(result);
        MessageExt messageExt = new MessageExt();
        messageExt.setBody(bytecode);
        List<MessageExt> lists = new ArrayList<>();
        lists.add(messageExt);

        Map map = avroConversion.convert(lists).get(0);

        logger.debug(map);
        this.romoveRedundancyOfJsonMap(map);

        ObjectMapper objectMapper = new ObjectMapper();
        logger.debug(objectMapper.writeValueAsString(map));


    }

    private void romoveRedundancyOfJsonMap(Map mapObject) {
        for(Object entry : mapObject.entrySet()) {
            logger.debug(((Map.Entry)entry).getValue().getClass());
            if (((Map.Entry)entry).getValue() instanceof Map) {
                Map.Entry valueEntry = (Map.Entry) ((Map)((Map.Entry)entry).getValue()).entrySet().iterator().next();
                String type = "map";
                if (valueEntry.getValue() instanceof List)
                    type = "array";

                mapObject.put(((Map.Entry)entry).getKey(), ((Map)((Map.Entry)entry).getValue()).get(type));
            }
        }
    }
}
