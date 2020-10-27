package cn.ac.iie.storm.bolt;


import cn.ac.iie.storm.utils.common.LogSendKafka;
import cn.ac.iie.storm.utils.file.StreamAggregateConfig;
import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * 发送到kafka的bolt
 *
 * @author qidaijie
 */
public class ResultSendBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 3237813470939823159L;
    private static Logger logger = Logger.getLogger(ResultSendBolt.class);
    private LogSendKafka logSendKafka;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        logSendKafka = LogSendKafka.getInstance();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        try {
            logSendKafka.sendMessage(tuple.getStringByField("message"));
        } catch (Exception e) {
            logger.error(StreamAggregateConfig.RESULTS_OUTPUT_TOPIC + "日志发送Kafka过程出现异常,异常信息:" + e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
