package me.liexing.connect;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import me.liexing.connect.mqconsumer.Consumer;
import me.liexing.connect.mqconsumer.MqConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by jinsheng on 16/9/18.
 */
public class MqTest {

    static Logger logger = LogManager.getLogger(MqTest.class);

    @JsonProperty("consumer")
    static Consumer consumer;

    public void setConsumer(Consumer consumer) {
        MqTest.consumer = consumer;
    }


    DefaultMQProducer producer;
    String topic;


    @Before
    public void before() {

        this.producer = new DefaultMQProducer("ProducerGroupName");
        this.producer.setNamesrvAddr("10.10.19.25:9876");
        this.producer.setInstanceName("Producer");


        this.topic = "test";

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);

        try {
            MqTest mqTest = objectMapper.readValue(this.getClass().getResourceAsStream("/property.json"), MqTest.class);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            this.producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    @After
    public void after() {
        ((MqConsumer)consumer).getConsumer().shutdown();
        this.producer.shutdown();
    }

    private void produce() {
        for (int i = 0; i < 100; i++) {
            Message msg = new Message(this.topic, "tag", "OrderID0034",// key
                    ("Hello MetaQ").getBytes());// body
            Message msg1 = new Message(this.topic + "test", "tag", "OrderID0034",// key
                    ("Hello MetaQ").getBytes());// body
            try {
                SendResult sendResult = this.producer.send(msg);
                logger.debug(sendResult);
                sendResult = this.producer.send(msg1);
                logger.debug(sendResult);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }


    public void testConsumer() {
        try {

            ((MqConsumer)consumer).setListener();
            ((MqConsumer)consumer).start();
        } catch (MQClientException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        this.produce();

        final Timer timer = new Timer("TimerThread", true);

        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                ((MqConsumer)consumer).getConsumer().shutdown();
                this.cancel();
            }
        }, 30000);
    }
}
