package me.liexing.connect.mqconsumer;


import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import me.liexing.connect.data.DataStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.List;

/**
 * Created by jinsheng on 16/9/18.
 */
public class MqConsumer implements Consumer{

    private DefaultMQPushConsumer consumer;

    static String topic;



    public static Logger logger = LogManager.getLogger(MqConsumer.class);

    //todo Singleton
    @JsonCreator
    public MqConsumer(
            @JsonProperty("ConsumerGroupName") String consumerGroupName,
            @JsonProperty("NamesrvAddr") String namesrvAddr,
            @JsonProperty("Topic") String topic,
            @JsonProperty("MessageBatchMaxSize") Integer messageBatchMaxSize) throws MQClientException {

        this.consumer = new DefaultMQPushConsumer(consumerGroupName);
        this.consumer.setNamesrvAddr(namesrvAddr);
        this.consumer.setInstanceName("Consumber");
        this.consumer.subscribe(topic, "*");
        this.consumer.setConsumeMessageBatchMaxSize(messageBatchMaxSize);


        MqConsumer.topic = topic;

    }

    public void setListener() throws MQClientException {
        this.consumer.registerMessageListener(new MessageListenerConcurrently() {

            public ConsumeConcurrentlyStatus consumeMessage(
                    List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                logger.info(Thread.currentThread().getName() + " Receive New Messages: " + msgs.size());
                try {
                    DataStore.sourceBlockingQueue.put(msgs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    logger.error("put data to source queue error: " + msgs.size());
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

    }

    public void start() throws MQClientException {
        this.consumer.start();
        logger.info("mq consumer start...");
    }

    public DefaultMQPushConsumer getConsumer() {
        return this.consumer;
    }
}