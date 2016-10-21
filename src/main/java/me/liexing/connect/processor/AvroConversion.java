package me.liexing.connect.processor;

import com.alibaba.rocketmq.common.message.MessageExt;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by jinsheng on 16/9/19.
 */
public class AvroConversion implements DataConversion {

    static Schema schema;

    static Logger logger = LogManager.getLogger(AvroConversion.class);

    private ObjectMapper objectMapper;

    public AvroConversion(String schemaFilename) throws IOException {
        this.objectMapper = new ObjectMapper();
        File file = new File(schemaFilename);
        if (file.exists()) {
            schema = new Schema.Parser().parse(file);
        }
        else {
            schema = new Schema.Parser().parse(Processor.class.getResourceAsStream(schemaFilename));
        }
    }

    public List<Map> convert(Object source) {
        List<Map> result = new ArrayList<>();
        for(MessageExt msg : (List<MessageExt>)source) {
            logger.debug(msg.getBody());

            Map map = null;
            try {
                String jsonStr = this.avrodecode((byte[]) msg.getBody());
                logger.debug(jsonStr);
                map = this.objectMapper.readValue(jsonStr, Map.class);
                romoveRedundancyOfJsonMap(map);
                result.add(map);
            } catch (Exception e) {
                logger.error("avro schema error. drop a message");
            }
        }
        return result;
    }

    public String avrodecode(byte[] source) {
        GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);
        boolean pretty = false;
        DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, output, pretty);
            Decoder decoder = DecoderFactory.get().binaryDecoder(source, null);
            Object datum = reader.read(null, decoder);
            writer.write(datum, encoder);
            encoder.flush();
            output.flush();
        } catch (Exception e) {
            logger.warn("avro decode error: " + source);
            return null;
        }
        String str = null;
        try {
            str = new String(output.toByteArray(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return str;
    }

    public byte[] avroencode(String jsonStr) {
        DatumReader<Object> reader = new GenericDatumReader<>(schema);
        GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonStr);
            Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
            Object datum = reader.read(null, decoder);
            writer.write(datum, encoder);
            encoder.flush();
        } catch (Exception e) {
            //e.printStackTrace();
            logger.error("avro encode error: " + jsonStr);
        }
        return output.toByteArray();
    }

    /**
     * 删除avro结构中冗余的数据，比如"mapName":{"map":{"key":"value"这种}}
     * @param mapObject
     */
    public static void romoveRedundancyOfJsonMap(Map mapObject) {
        for(Object entry : mapObject.entrySet()) {
            //logger.debug(((Map.Entry)entry).getValue().getClass());
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
