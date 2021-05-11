package com.zdjizhi.storm.bolt;

import com.zdjizhi.storm.utils.common.StreamAggregateConfig;
import com.zdjizhi.storm.utils.exception.AggregationException;
import com.zdjizhi.storm.utils.http.HttpClientUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zdjizhi.utils.StringUtil;
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

import static com.zdjizhi.storm.utils.combine.AggregateUtils.transDimensions;

/**
 * @ClassNameMyWindowBolt
 * @Author lixkvip@126.com
 * @Date2020/6/9 14:45
 * @Version V1.0
 **/
public class ParseKvBolt extends BaseBasicBolt {
    private static final Log logger = LogFactory.get();
    private static final long serialVersionUID = -999382396035310355L;
    private JSONArray transforms;
    private JSONArray dimensions;
//    private static HashMap<Integer, String> appMap = new HashMap<>(32);


    /**
     * 此方法只在程序启动的时候执行一次，用来初始化
     *
     * @param stormConf Map
     * @param context   TopologyContext
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        String schema = HttpClientUtil.requestByGetMethod(StreamAggregateConfig.SCHEMA_HTTP);
        JSONObject jsonObject = JSONObject.parseObject(schema);
        String data = JSONObject.parseObject(jsonObject.getString("data")).getString("doc");
        //TODO 解析 schema
        transforms = JSONObject.parseArray(JSONObject.parseObject(data).getString("transforms"));

        //TODO 获取dimensions
        dimensions = JSONObject.parseArray(JSONObject.parseObject(data).getString("dimensions"));
//        updateAppRelation(appMap);

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            if (TupleUtils.isTick(tuple)) {
//                updateAppRelation(appMap);
            } else {
                String source = tuple.getStringByField("source");
                if (StringUtil.isNotBlank(source)) {
                    //TODO 解析tuple的 message
                    JSONObject message = JSONObject.parseObject(tuple.getStringByField("source"));
                    String protocolLabel = message.getString("common_protocol_label");

                    if (StringUtil.isNotBlank(protocolLabel)) {

                        //TODO 新建一个dimensions的Json对象
                        JSONObject dimensionsObj = transDimensions(dimensions, message);

                        for (Object transform : transforms) {
                            JSONObject transformObj = JSONObject.parseObject(transform.toString());
                            String function = transformObj.getString("function");
                            String name = transformObj.getString("name");
                            String fieldName = transformObj.getString("fieldName");
                            String parameters = transformObj.getString("parameters");

                            switch (function) {
                                case "dismantling":
                                    if (StringUtil.isNotBlank(parameters)) {
                                        if (message.containsKey(fieldName)) {
                                            dismantlingUtils(parameters, message, name, fieldName);
                                        }
                                    }
                                    break;
                                case "combination":
                                    if (StringUtil.isNotBlank(parameters)) {
                                        if (message.containsKey(fieldName)) {
                                            combinationUtils(parameters, message, name, fieldName, dimensionsObj);
                                        }
                                    }
                                    break;
                                case "hierarchy":
//                                    if (StringUtil.isNotBlank(parameters)) {
                                    collector.emit(new Values(dimensionsObj.getString(name), dimensionsObj.toString(), message.toString()));
//                                    }
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                }
            }
        } catch (AggregationException e) {
            logger.error("上层解析原始日志/拼接计算日志发送异常,异常信息:" + e);
        }

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>(16);
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, StreamAggregateConfig.UPDATE_APP_ID_TIME);
        return conf;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("label", "dimensions", "message"));
    }

    /**
     * alignment ID替换操作
     * 根据缓存中的AppId对应信息，获取当前AppId对应的具体名称。
     *
     * @param parameters 参数集
     * @param message    原始日志
     * @param name       结果日志列名
     * @param fieldName  原始日志列名
     */
    private static void dismantlingUtils(String parameters, JSONObject message, String name, String fieldName) {
        String[] alignmentPars = parameters.split(StreamAggregateConfig.FORMAT_SPLITTER);
        String commonAppId = message.getString(fieldName);
        int digits = Integer.parseInt(alignmentPars[0]);
        String appName = commonAppId.split(StreamAggregateConfig.PROTOCOL_SPLITTER)[digits];
        message.put(name, appName);
    }

    /**
     * combination 拼接操作
     * 获取方法函数中 parameters 字段，结构 "parameters": "abc,/" ;abc为要拼接字段 /为拼接的分隔符
     *
     * @param parameters    参数集
     * @param message       原始日志
     * @param name          结果日志列名
     * @param fieldName     原始日志列名
     * @param dimensionsObj 结果集
     */
    private static void combinationUtils(String parameters, JSONObject message, String name, String fieldName, JSONObject dimensionsObj) {
        String[] combinationPars = parameters.split(StreamAggregateConfig.FORMAT_SPLITTER);
        String combinationField = message.getString(combinationPars[0]);
        String splitter = combinationPars[1];
        if (StringUtil.isNotBlank(combinationField)) {
            String combinationValue = message.getString(fieldName) + splitter + combinationField;
            message.put(fieldName, combinationValue);
            dimensionsObj.put(name, combinationValue);
        }
    }
}
