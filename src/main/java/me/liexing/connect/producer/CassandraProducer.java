package me.liexing.connect.producer;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import me.liexing.connect.mqconsumer.MqConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.*;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by jinsheng on 16/9/18.
 */
public class CassandraProducer implements Producer{

    private String keyspace;
    private String coordinatorAddress;
    private String tableName;
    private String timestampKey;
    private String timestampFieldKey;
    private String filterHasField;
    private String filterHasNoField;
    private Boolean isUpdate;
    private List<String> updateFields;
    private String updatePrimaryKey;

    private List<ColumnMetadata> columns;

    private Cluster cluster;
    private Session session;

    private ObjectMapper objectMapper;

    static Logger logger = LogManager.getLogger(CassandraProducer.class);

    @JsonCreator
    public CassandraProducer(@JsonProperty("Keyspace") String keyspace,
                             @JsonProperty("CoordinatorAddress") String coordinatorAddress,
                             @JsonProperty("UserName") String userName,
                             @JsonProperty("Password") String password,
                             @JsonProperty("TableName") String tableName,
                             @JsonProperty("TimestampKey") String timestampKey,
                             @JsonProperty("TimestampFieldKey") String timestampFieldKey,
                             @JsonProperty("filterHasField") String filterHasField,
                             @JsonProperty("filterHasNoField") String filterHasNoField,
                             @JsonProperty("isUpdate") Boolean isUpdate,
                             @JsonProperty("updateFields") List<String> updateFields,
                             @JsonProperty("updatePrimaryKey") String updatePrimaryKey) {
        this.keyspace = keyspace;
        this.coordinatorAddress = coordinatorAddress;
        this.tableName = tableName;
        this.timestampKey = timestampKey;
        this.timestampFieldKey = timestampFieldKey;
        this.filterHasField = filterHasField;
        this.filterHasNoField = filterHasNoField;
        this.isUpdate = isUpdate;
        this.updateFields = updateFields;
        this.updatePrimaryKey = updatePrimaryKey;


        QueryOptions options = new QueryOptions();
        options.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
        if (userName.length() > 0)
            this.cluster = Cluster.builder().withCredentials(userName, password).addContactPoint(this.coordinatorAddress).withQueryOptions(options).build();
        else
            this.cluster = Cluster.builder().addContactPoint(this.coordinatorAddress).withQueryOptions(options).build();
        this.session = this.cluster.connect(this.keyspace);
        this.columns = this.cluster.getMetadata().getKeyspace(this.keyspace).getTable(this.tableName).getColumns();

        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    }

    public Session getSession() {
        return this.session;
    }

    private List<String> getColumnsStr() {
        StringBuffer sb = new StringBuffer();
        StringBuffer sbcount = new StringBuffer();
        for (ColumnMetadata column : this.columns) {
            sb.append(column.getName() + ",");
            sbcount.append("?,");
        }
        sb.deleteCharAt(sb.length()-1);
        sbcount.deleteCharAt(sbcount.length()-1);
        List<String> result = new ArrayList<String>();
        result.add(sb.toString());
        result.add(sbcount.toString());
        return result;
    }

    //todo
    private String getupdateStr() {
        StringBuffer sb = new StringBuffer();
        for (String updateField : updateFields) {
            sb.append(updateField + "=?,");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    private String getUpdateStrWithValue(Map map) {
        StringBuffer sb = new StringBuffer();
        for (String updateField : updateFields) {
            sb.append(updateField + "=" + map.get(updateField) + ",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    /**
     * 发送数据，传入参数是List&lt;String&gt;类型，String中为json字符串。
     * @param jsonList
     * @throws IOException
     */
    public void pushData(List<Object> jsonList) throws IOException {
        BatchStatement batchStatement = new BatchStatement();
        PreparedStatement insert_statement = session.prepare("INSERT INTO " + this.tableName + " JSON ?");
        logger.info("write data to cassandra: " + jsonList.size());
        for (Object str: jsonList) {
            if (this.timestampFieldKey.length() > 0) {
                ((Map)str).put(timestampFieldKey, new Date().getTime());
            }
            boolean dropflag = false;
            if (this.filterHasField.length() > 0 && ((List)(((Map)str).get(this.filterHasField))).size() > 0)
                if (this.filterHasNoField.length() > 0 && ((List)(((Map)str).get(this.filterHasNoField))).size() == 0)
                    dropflag = true;
            if (dropflag) {
                logger.error("drop a message, drop flag is true");
                logger.error(objectMapper.writeValueAsString(str));
                System.exit(-1);
                continue;
            }

            BoundStatement bs = insert_statement.bind(objectMapper.writeValueAsString(str));
            if (this.timestampKey.length() > 0 && ((Map) str).containsKey(this.timestampKey))
                bs.setDefaultTimestamp((Long) ((Map) str).get(this.timestampKey) * 1000000); // second to microsecond
            logger.debug(objectMapper.writeValueAsString(str));
            batchStatement.add(bs);
        }
        logger.debug(insert_statement.getQueryString());
        ResultSet result = session.execute(batchStatement);
        batchStatement.clear();
        logger.debug(result.toString());
    }

    public void updateData(List<Object> lists) {

        BatchStatement batchStatement = new BatchStatement();
        for (Object map : lists) {
            String sql = "update " + this.tableName  + " set " + getUpdateStrWithValue((Map) map) + " where " + this.updatePrimaryKey + "='" + ((Map)map).get(this.updatePrimaryKey) + "'";// IF EXISTS";
            logger.info(sql);
            SimpleStatement simpleStatement = new SimpleStatement(sql);
            batchStatement.add(simpleStatement);
        }
        session.execute(batchStatement);
        batchStatement.clear();
    }

    public void close() {
        this.session.close();
        this.cluster.close();
    }

    public Boolean getIsUpdate() {
        return this.isUpdate;
    }

    public List<ColumnMetadata> getColumns() {
        return columns;
    }
}
