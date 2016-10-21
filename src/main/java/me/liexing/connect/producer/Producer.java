package me.liexing.connect.producer;

import me.liexing.connect.mqconsumer.MqConsumer;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.io.IOException;
import java.util.List;

/**
 * Created by jinsheng on 2016/9/22.
 */

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name="cassandra", value=CassandraProducer.class)
})
public interface Producer {

    void pushData(List<Object> dataLists) throws IOException;

    void updateData(List<Object> lists);

    void close();
}
