package me.liexing.connect.processor;

import com.alibaba.rocketmq.client.exception.MQClientException;
import me.liexing.connect.data.DataStore;
import me.liexing.connect.mqconsumer.Consumer;
import me.liexing.connect.mqconsumer.MqConsumer;
import me.liexing.connect.producer.CassandraProducer;
import me.liexing.connect.producer.Producer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by jinsheng on 16/9/19.
 */
public class Processor {

    @JsonProperty("consumer")
    static Consumer consumer;

    @JsonProperty("producer")
    static Producer producer;

    static DataStore dataStore;

    static DataConversionHandler dataConversionHandler;

    static Logger logger = LogManager.getLogger(Processor.class);


    public void setConsumer(Consumer consumer) {
        Processor.consumer = consumer;
    }

    public void setProducer(Producer producer) {
        Processor.producer = producer;
    }


    public static void main(String[] argv) {

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);

        Processor processor;
        try {

            File file = new File("property.json");
            if (file.exists()) {
                processor = objectMapper.readValue(file, Processor.class);
                dataStore = objectMapper.readValue(file, DataStore.class);
                dataConversionHandler = objectMapper.readValue(file, DataConversionHandler.class);
            }
            else {
                processor = objectMapper.readValue(Processor.class.getResourceAsStream("/property.json"),Processor.class);
                dataStore =  objectMapper.readValue(Processor.class.getResourceAsStream("/property.json"), DataStore.class);
                dataConversionHandler = objectMapper.readValue(Processor.class.getResourceAsStream("/property.json"), DataConversionHandler.class);

            }


            //mq监听
            consumer.setListener();
            consumer.start();

            //数据转换
            new Thread(dataConversionHandler).start();

            //数据发送
            while (true) {
                List<Object> lists = new ArrayList<>();
                int count = DataStore.producerBlockingQueue.drainTo(lists);
                if (count > 0) {
                    try {
                        if (((CassandraProducer)producer).getIsUpdate())
                            producer.updateData(lists);
                        else
                            producer.pushData(lists);
                    }
                    catch (Exception e) {
                        logger.error("error push data...");
                        logger.error(e.getMessage());
                    }

                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }

    }
}
