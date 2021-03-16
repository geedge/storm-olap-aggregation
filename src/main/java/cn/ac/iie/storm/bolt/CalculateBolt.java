package cn.ac.iie.storm.bolt;

import cn.ac.iie.storm.utils.combine.AggregateUtils;
import cn.ac.iie.storm.utils.file.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassNameAggregateBolt
 * @Author lixkvip@126.com
 * @Date2020/6/24 13:39
 * @Version V1.0
 **/
public class CalculateBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(CalculateBolt.class);
    private static final long serialVersionUID = -7666031217706448622L;
    private HashMap<String, JSONObject> metricsMap;
    private HashMap<String, String[]> actionMap;
    private HashMap<String, JSONObject> cacheMap;

    /**
     * 只会在程序初始化的时候执行一次
     *
     * @param stormConf
     * @param context
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        String schema = HttpClientUtil.requestByGetMethod(StreamAggregateConfig.SCHEMA_HTTP);
        cacheMap = new HashMap<>(32);

        // TODO 获取action标签的内容
        actionMap = AggregateUtils.getActionMap(schema);
        metricsMap = AggregateUtils.getMetircsMap(schema);

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            if (TupleUtils.isTick(input)) {
                for (String key : cacheMap.keySet()) {
                    collector.emit(new Values(key, cacheMap.get(key).toString()));
                }
                cacheMap.clear();

            } else {
                String label = input.getStringByField("label");
                //action中某个协议的所有function,如果没有就默认
                String[] metrics = actionMap.getOrDefault(label, actionMap.get("Default"));

                String dimensions = input.getStringByField("dimensions");
                String message = input.getStringByField("message");

                //一条数据
                JSONObject event = JSONObject.parseObject(message);
                //数据中的key值 (protocol,device_id,isp)
                //map中对应的数据，可能为空,为空就默认创建一个对象
                JSONObject cacheMessage = cacheMap.getOrDefault(dimensions, new JSONObject());
                //TODO 接下来遍历所有的函数，去metrics的Map中去找对应的方法，并执行
                for (String metric : metrics) {
                    String name = metricsMap.get(metric).getString("name");
                    //可能为空
                    String fieldName = metricsMap.get(name).getString("fieldName");

                    //TODO 每次新增函数，需要改动此处代码
                    functionSet(name, cacheMessage, cacheMessage.getString(name), event.getString(fieldName));
                }
                cacheMap.put(dimensions, cacheMessage);

            }
        } catch (Exception e) {
            logger.error("计算节点异常,异常信息:" + e);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("dimensions", "message"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>(16);
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, StreamAggregateConfig.FIRST_AGG_TIME);
        return conf;
    }

    /**
     * 根据schema描述对应字段进行操作的 函数集合
     *
     * @param name           函数名称
     * @param cacheMessage   结果集
     * @param nameValue      当前值
     * @param fieldNameValue 新加值
     */
    private static void functionSet(String name, JSONObject cacheMessage, String nameValue, String fieldNameValue) {
        switch (name) {
            case "sessions":
                cacheMessage.put(name, AggregateUtils.longSum(nameValue, fieldNameValue));
                break;
            case "c2s_byte_num":
                cacheMessage.put(name, AggregateUtils.longSum(nameValue, fieldNameValue));
                break;
            case "s2c_byte_num":
                cacheMessage.put(name, AggregateUtils.longSum(nameValue, fieldNameValue));
                break;
            case "c2s_pkt_num":
                cacheMessage.put(name, AggregateUtils.longSum(nameValue, fieldNameValue));
                break;
            case "s2c_pkt_num":
                cacheMessage.put(name, AggregateUtils.longSum(nameValue, fieldNameValue));
                break;
            case "c2s_ipfrag_num":
                cacheMessage.put(name, AggregateUtils.longSum(nameValue, fieldNameValue));
                break;
            case "s2c_ipfrag_num":
                cacheMessage.put(name, AggregateUtils.longSum(nameValue, fieldNameValue));
                break;
            case "s2c_tcp_lostlen":
                cacheMessage.put(name, AggregateUtils.longSum(nameValue, fieldNameValue));
                break;
            case "c2s_tcp_lostlen":
                cacheMessage.put(name, AggregateUtils.longSum(nameValue, fieldNameValue));
                break;
            case "c2s_tcp_unorder_num":
                cacheMessage.put(name, AggregateUtils.longSum(nameValue, fieldNameValue));
                break;
            case "s2c_tcp_unorder_num":
                cacheMessage.put(name, AggregateUtils.longSum(nameValue, fieldNameValue));
                break;

            case "unique_sip_num":
                //TODO
                //cacheMessage.put(name, AggregateUtils.)
                break;
            case "unique_cip_num":
                //TODO
                break;
            default:
                break;
        }
    }


//     cacheMessage.put("sessions", AggregateUtils.longSum(cacheMessage.getString("sessions"), event.getString("common_sessions")));
//                cacheMessage.put("c2s_byte_num", AggregateUtils.longSum(cacheMessage.getString("c2s_byte_num"), event.getString("common_c2s_byte_num")));
//                cacheMessage.put("s2c_byte_num", AggregateUtils.longSum(cacheMessage.getString("s2c_byte_num"), event.getString("common_s2c_byte_num")));
//                cacheMessage.put("c2s_pkt_num", AggregateUtils.longSum(cacheMessage.getString("c2s_pkt_num"), event.getString("common_c2s_pkt_num")));
//                cacheMessage.put("s2c_pkt_num", AggregateUtils.longSum(cacheMessage.getString("s2c_pkt_num"), event.getString("common_s2c_pkt_num")));
//                cacheMessage.put("c2s_ipfrag_num", AggregateUtils.longSum(cacheMessage.getString("c2s_ipfrag_num"), event.getString("common_c2s_ipfrag_num")));
//                cacheMessage.put("s2c_ipfrag_num", AggregateUtils.longSum(cacheMessage.getString("s2c_ipfrag_num"), event.getString("common_s2c_ipfrag_num")));
//                cacheMessage.put("c2s_tcp_lostlen", AggregateUtils.longSum(cacheMessage.getString("c2s_tcp_lostlen"), event.getString("common_c2s_tcp_lostlen")));
//                cacheMessage.put("s2c_tcp_lostlen", AggregateUtils.longSum(cacheMessage.getString("s2c_tcp_lostlen"), event.getString("common_s2c_tcp_lostlen")));
//                cacheMessage.put("c2s_tcp_unorder_num", AggregateUtils.longSum(cacheMessage.getString("c2s_tcp_unorder_num"), event.getString("common_c2s_tcp_unorder_num")));
//                cacheMessage.put("s2c_tcp_unorder_num", AggregateUtils.longSum(cacheMessage.getString("s2c_tcp_unorder_num"), event.getString("common_s2c_tcp_unorder_num")));
}
