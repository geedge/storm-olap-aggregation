package cn.ac.iie.storm.bolt;


import cn.ac.iie.storm.utils.common.LogSendKafka;
import cn.ac.iie.storm.utils.logout.LogPrintUtil;
import cn.ac.iie.storm.utils.system.StreamAggregateConfig;
import com.alibaba.fastjson.JSONObject;
import com.zdjizhi.utils.StringUtil;
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
            String message = tuple.getStringByField("message");
            if (StringUtil.isNotBlank(message)) {
                JSONObject jsonObject = JSONObject.parseObject(message);
                StringBuffer stringBuffer = new StringBuffer();
                String[] protocolIds = jsonObject.getString("protocol_id").split("/");
                for (int i = (protocolIds.length - 1); i >= 0; i--) {
                    if (StringUtil.isBlank(stringBuffer.toString())) {
                        stringBuffer.append(protocolIds[i]);
                        jsonObject.put("protocol_id", stringBuffer.toString());
                        logSendKafka.sendMessage(jsonObject.toString());
                    } else {
                        stringBuffer.append("/").append(protocolIds[i]);
                        jsonObject.put("protocol_id", stringBuffer.toString());
                        logSendKafka.sendMessage(jsonObject.toString());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("层级拆分或发送Kakfa异常,异常信息:" + LogPrintUtil.print(e));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
