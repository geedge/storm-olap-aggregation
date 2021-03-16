package com.zdjizhi.storm.utils.kafka;


import com.zdjizhi.storm.utils.common.StreamAggregateConfig;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * NTC系统配置产生日志写入数据中心类
 *
 * @author Administrator
 * @create 2018-08-13 15:11
 */

public class LogSendKafka {

    private static final Log logger = LogFactory.get();

    /**
     * kafka生产者，用于向kafka中发送消息
     */
    private static Producer<String, String> kafkaProducer;

    /**
     * kafka生产者适配器（单例），用来代理kafka生产者发送消息
     */
    private static LogSendKafka logSendKafka;

    private LogSendKafka() {
        initKafkaProducer();
    }

    public static LogSendKafka getInstance() {
        if (logSendKafka == null) {
            logSendKafka = new LogSendKafka();
        }
        return logSendKafka;
    }


    public void sendMessage(String message) {
        final int[] errorSum = {0};
        kafkaProducer.send(new ProducerRecord<>(StreamAggregateConfig.RESULTS_OUTPUT_TOPIC, message), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    logger.error("写入" + StreamAggregateConfig.RESULTS_OUTPUT_TOPIC + "出现异常", exception);
                    errorSum[0]++;
                }
            }
        });

        kafkaProducer.flush();
        logger.debug("Log sent to National Center successfully!!!!!");
    }

    /**
     * 根据kafka生产者配置信息初始化kafka消息生产者,只初始化一次
     */
    private void initKafkaProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", StreamAggregateConfig.RESULTS_BOOTSTRAP_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "1");
        properties.put("linger.ms", "2");
        properties.put("request.timeout.ms", 30000);
        properties.put("batch.size", 262144);
        properties.put("buffer.memory", 33554432);

        /**
         * 限流配置-20201117
         */
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, StreamAggregateConfig.PRODUCER_CLIENT_ID);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, StreamAggregateConfig.PRODUCER_KAFKA_COMPRESSION_TYPE);

        kafkaProducer = new KafkaProducer<>(properties);
    }

}
