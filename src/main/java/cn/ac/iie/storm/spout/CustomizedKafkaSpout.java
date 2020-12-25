package cn.ac.iie.storm.spout;

import cn.ac.iie.storm.utils.file.StreamAggregateConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * kafkaSpout
 *
 * @author Administrator
 */
public class CustomizedKafkaSpout extends BaseRichSpout {
    private static final long serialVersionUID = -3363788553406229592L;
    private KafkaConsumer<String, String> consumer;
    private SpoutOutputCollector collector = null;
    private TopologyContext context = null;
    private final static Logger logger = Logger.getLogger(CustomizedKafkaSpout.class);


    private static Properties createConsumerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", StreamAggregateConfig.BOOTSTRAP_SERVERS);
        props.put("group.id", StreamAggregateConfig.GROUP_ID);
        props.put("session.timeout.ms", "60000");
        props.put("max.poll.records", 3000);
        props.put("max.partition.fetch.bytes", 31457280);
        props.put("auto.offset.reset", StreamAggregateConfig.AUTO_OFFSET_RESET);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        /**
         * 限流配置-20201117
         */
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, StreamAggregateConfig.CONSUMER_CLIENT_ID);
        return props;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // TODO Auto-generated method stub
        this.collector = collector;
        this.context = context;
        Properties prop = createConsumerConfig();
        this.consumer = new KafkaConsumer<>(prop);
        this.consumer.subscribe(Collections.singletonList(StreamAggregateConfig.KAFKA_TOPIC));
    }

    @Override
    public void close() {
        consumer.close();
    }

    @Override
    public void nextTuple() {
        try {
            // TODO Auto-generated method stub
            ConsumerRecords<String, String> records = consumer.poll(10000L);
            Thread.sleep(StreamAggregateConfig.TOPOLOGY_SPOUT_SLEEP_TIME);
            for (ConsumerRecord<String, String> record : records) {
                this.collector.emit(new Values(record.value()));
            }
        } catch (Exception e) {
            logger.error("KafkaSpout发送消息出现异常!", e);
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        declarer.declare(new Fields("source"));
    }
}
