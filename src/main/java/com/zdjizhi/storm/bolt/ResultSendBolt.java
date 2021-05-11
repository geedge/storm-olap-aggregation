package com.zdjizhi.storm.bolt;


import com.zdjizhi.storm.utils.combine.AggregateUtils;
import com.zdjizhi.storm.utils.exception.AggregationException;
import com.zdjizhi.storm.utils.http.HttpClientUtil;
import com.zdjizhi.storm.utils.kafka.LogSendKafka;
import com.zdjizhi.storm.utils.common.StreamAggregateConfig;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.fastjson.JSONObject;
import com.zdjizhi.utils.StringUtil;
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
    private static final Log logger = LogFactory.get();
    private LogSendKafka logSendKafka;
    private static String regex;
    private static String name;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        String schema = HttpClientUtil.requestByGetMethod(StreamAggregateConfig.SCHEMA_HTTP);
        logSendKafka = LogSendKafka.getInstance();
        regex = AggregateUtils.getHierarchy(schema, "parameters");
        name = AggregateUtils.getHierarchy(schema, "name");
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        try {
            String message = tuple.getStringByField("message");
            if (StringUtil.isNotBlank(message)) {
                JSONObject jsonObject = JSONObject.parseObject(message);
                StringBuffer stringBuffer = new StringBuffer();
                String[] protocolIds = jsonObject.getString(name).split(StreamAggregateConfig.PROTOCOL_SPLITTER);
                for (String proto : protocolIds) {
                    if (StringUtil.isBlank(stringBuffer.toString())) {
                        stringBuffer.append(proto);
                        jsonObject.put(name, stringBuffer.toString());
                        logSendKafka.sendMessage(jsonObject.toString());
                    } else {
                        stringBuffer.append(regex).append(proto);
                        jsonObject.put(name, stringBuffer.toString());
                        logSendKafka.sendMessage(jsonObject.toString());
                    }
                }
            }
        } catch (AggregationException e) {
            logger.error("日志发送Kafka过程出现异常,异常信息:" + e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
