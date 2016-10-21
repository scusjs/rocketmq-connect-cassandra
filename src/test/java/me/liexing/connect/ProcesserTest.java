package me.liexing.connect;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import me.liexing.connect.data.DataStore;
import me.liexing.connect.mqconsumer.Consumer;
import me.liexing.connect.mqconsumer.MqConsumer;
import me.liexing.connect.processor.AvroConversion;
import me.liexing.connect.processor.DataConversionHandler;
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
import java.util.List;
import java.util.Map;

/**
 * Created by jinsheng on 16/9/20.
 */
public class ProcesserTest {

    @JsonProperty("consumer")
    static Consumer consumer;

    @JsonProperty("producer")
    static Producer producer;

    static DataStore dataStore;

    static DataConversionHandler dataConversionHandler;

    public void setConsumer(Consumer consumer) {
        ProcesserTest.consumer = consumer;
    }

    public void setProducer(Producer producer) {
        ProcesserTest.producer = producer;
    }

    DefaultMQProducer defaultMQProducer;

    private static Logger logger = LogManager.getLogger(ProcesserTest.class);

    @Before
    public void create() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);

        ProcesserTest processorTest;
        try {
            processorTest = objectMapper.readValue(Processor.class.getResourceAsStream("/property.json"),ProcesserTest.class);
            dataStore =  objectMapper.readValue(Processor.class.getResourceAsStream("/property.json"), DataStore.class);
            dataConversionHandler = objectMapper.readValue(Processor.class.getResourceAsStream("/property.json"), DataConversionHandler.class);

            //mq监听
            consumer.setListener();
            consumer.start();

            this.defaultMQProducer = new DefaultMQProducer("ProducerGroupName1");
            this.defaultMQProducer.setNamesrvAddr("10.10.19.25:9876");
            this.defaultMQProducer.setInstanceName("Producer");
            this.defaultMQProducer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendData() {
        String testJson1 = "{\"name1\":\"zhangsan3\",\"id\":1,\"map1\":{\"map\":{\"key\":\"value\"}}}";
        byte[] bytecode1 = ((AvroConversion)dataConversionHandler.getDataConversion()).avroencode(testJson1);

        String testJson2 = "{\"name1\":\"lisi3\",\"id\":1,\"map1\":{\"map\":{\"key\":\"value\"}}}";
        byte[] bytecode2 = ((AvroConversion)dataConversionHandler.getDataConversion()).avroencode(testJson2);

        String testJson3 = "{\"name1\":\"wangwu3\",\"id\":1,\"map1\":{\"map\":{\"key\":\"value\"}}}";
        byte[] bytecode3 = ((AvroConversion)dataConversionHandler.getDataConversion()).avroencode(testJson3);

        logger.debug(bytecode1);
        logger.debug(bytecode2);
        logger.debug(bytecode3);

        try {
            Message msg1 = new Message("test",  bytecode1);
            SendResult sendResult = this.defaultMQProducer.send(msg1);

            Message msg2 = new Message("test",  bytecode2);
            this.defaultMQProducer.send(msg2);
            Message msg3 = new Message("test",  bytecode3);
            this.defaultMQProducer.send(msg3);
            logger.debug(sendResult);
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @After
    public void end() {
        ((MqConsumer)consumer).getConsumer().shutdown();
        defaultMQProducer.shutdown();
        producer.close();
    }


    @Test
    public void dataConvert() throws InterruptedException, IOException {
        this.sendData();
        //数据转换
        new Thread(dataConversionHandler).start();

        Map<String, Object> data1 = (Map<String, Object>) DataStore.producerBlockingQueue.take();
        Map<String, Object> data2 = (Map<String, Object>) DataStore.producerBlockingQueue.take();
        Map<String, Object> data3 = (Map<String, Object>) DataStore.producerBlockingQueue.take();

        List<Object> list = new ArrayList<>();
        list.add(data1);
        list.add(data2);
        list.add(data3);

        producer.pushData(list);

        this.testGet();

    }

    private void testGet() {
        ResultSet resultSet = ((CassandraProducer)producer).getSession().execute("select * from testtable");
        for (Row row : resultSet) {
            logger.debug(row.getString("name"));
        }
    }
}
