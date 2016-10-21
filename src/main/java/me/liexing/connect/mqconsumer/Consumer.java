package me.liexing.connect.mqconsumer;

import org.codehaus.jackson.annotate.*;

/**
 * Created by jinsheng on 2016/9/22.
 */

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name="mq", value=MqConsumer.class)
})
public interface Consumer {

    void setListener() throws Exception;

    void start() throws Exception;
}
