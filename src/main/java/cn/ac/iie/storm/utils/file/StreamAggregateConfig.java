package cn.ac.iie.storm.utils.file;


/**
 * @author Administrator
 */
public class StreamAggregateConfig {

    public static final String FORMAT_SPLITTER = ",";
    /**
     * System
     */
    public static final Integer SPOUT_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0, "spout.parallelism");
    public static final Integer CALCULATE_BOLT_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0, "calculate.bolt.parallelism");
    public static final Integer GATHERING_BOLT_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0, "gathering.bolt.parallelism");
    public static final Integer PARSE_BOLT_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0, "parse.bolt.parallelism");
    public static final Integer TOPOLOGY_WORKERS = StreamAggregateConfigurations.getIntProperty(0, "topology.workers");
    public static final Integer KAFKA_BOLT_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0, "kafka.bolt.parallelism");
    public static final Integer TOPOLOGY_NUM_ACKS = StreamAggregateConfigurations.getIntProperty(0, "topology.num.acks");
    public static final Integer TOPOLOGY_SPOUT_SLEEP_TIME = StreamAggregateConfigurations.getIntProperty(0, "topology.spout.sleep.time");
    public static final Integer BATCH_INSERT_NUM = StreamAggregateConfigurations.getIntProperty(0, "batch.insert.num");
    public static final Integer DATA_CENTER_ID_NUM = StreamAggregateConfigurations.getIntProperty(0, "data.center.id.num");
    public static final Integer MAX_FAILURE_NUM = StreamAggregateConfigurations.getIntProperty(0, "max.failure.num");


    public static final Integer FIRST_AGG_TIME = StreamAggregateConfigurations.getIntProperty(0, "first.agg.time");
    public static final Integer SECOND_AGG_TIME = StreamAggregateConfigurations.getIntProperty(0, "second.agg.time");
    public static final Integer UPDATE_APP_ID_TIME = StreamAggregateConfigurations.getIntProperty(0, "update.app.id.time");


    /**
     * kafka
     */
    public static final String BOOTSTRAP_SERVERS = StreamAggregateConfigurations.getStringProperty(0, "bootstrap.servers");
    public static final String RESULTS_BOOTSTRAP_SERVERS = StreamAggregateConfigurations.getStringProperty(0, "results.bootstrap.servers");
    public static final String GROUP_ID = StreamAggregateConfigurations.getStringProperty(0, "group.id");
    public static final String RESULTS_OUTPUT_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "results.output.topic");
    public static final String KAFKA_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "kafka.topic");
    public static final String AUTO_OFFSET_RESET = StreamAggregateConfigurations.getStringProperty(0, "auto.offset.reset");

    /**
     * kafka限流配置-20201117
     */
    public static final String PRODUCER_KAFKA_COMPRESSION_TYPE = StreamAggregateConfigurations.getStringProperty(0, "producer.kafka.compression.type");
    public static final String CONSUMER_CLIENT_ID = StreamAggregateConfigurations.getStringProperty(0, "consumer.client.id");
    public static final String PRODUCER_CLIENT_ID = StreamAggregateConfigurations.getStringProperty(0, "producer.client.id");

    /**
     * http
     */
    public static final String SCHEMA_HTTP = StreamAggregateConfigurations.getStringProperty(0, "schema.http");
    public static final String APP_ID_HTTP = StreamAggregateConfigurations.getStringProperty(0, "app.id.http");

}