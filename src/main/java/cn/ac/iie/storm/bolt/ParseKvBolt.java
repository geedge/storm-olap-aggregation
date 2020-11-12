package cn.ac.iie.storm.bolt;


import cn.ac.iie.storm.utils.combine.AggregateUtils;
import cn.ac.iie.storm.utils.file.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zdjizhi.utils.StringUtil;
import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static cn.ac.iie.storm.utils.combine.AggregateUtils.transDimensions;
import static cn.ac.iie.storm.utils.combine.AggregateUtils.updateAppRelation;

/**
 * @ClassNameMyWindowBolt
 * @Author lixkvip@126.com
 * @Date2020/6/9 14:45
 * @Version V1.0
 **/
public class ParseKvBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(ParseKvBolt.class);
    private static final long serialVersionUID = -999382396035310355L;
    private JSONArray transforms;
    private JSONArray dimensions;
    private static HashMap<Long, String> appMap = new HashMap<>(32);


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
        updateAppRelation(appMap);

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            if (TupleUtils.isTick(tuple)) {
                updateAppRelation(appMap);
            } else {
                //TODO 解析tuple的 message
                JSONObject message = JSONObject.parseObject(tuple.getStringByField("source"));

                //TODO 新建一个dimensions的Json对象
                JSONObject dimensionsObj = transDimensions(dimensions, message);

                for (Object transform : transforms) {
                    JSONObject transformObj = JSONObject.parseObject(transform.toString());
                    String function = transformObj.getString("function");
                    String name = transformObj.getString("name");
                    String fieldName = transformObj.getString("fieldName");
                    String parameters = transformObj.getString("parameters");

                    switch (function) {
                        case "alignment":
                            if (StringUtil.isNotBlank(parameters)) {
                                if (message.containsKey(fieldName)) {
                                    alignmentUtils(parameters, message, name, fieldName);
                                }
                            }
                            break;
                        case "combination":
                            if (StringUtil.isNotBlank(parameters)) {
                                combinationUtils(parameters, message, name, fieldName, dimensionsObj);
                            }
                            break;
                        case "hierarchy":
                            String hierarchyValue = message.getString(fieldName);
                            //TODO 执行拆分代码
                            if (StringUtil.isNotBlank(hierarchyValue) && StringUtil.isNotBlank(parameters)) {
                                String[] hierarchyPars = parameters.split(StreamAggregateConfig.FORMAT_SPLITTER);
                                String[] dimensionsArr = hierarchyValue.split(hierarchyPars[0]);
                                //TODO 递归拼接tuple并发送出去
                                AggregateUtils.reSend(1, dimensionsArr, "", collector, message, dimensionsObj, name);
                            }
                            break;
                        default:
                            //数据原样输出
                            collector.emit(new Values(null, null, message.toString()));
                            break;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("上层解析原始日志/拼接计算日志发送异常,异常信息:" + e);
            e.printStackTrace();
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
     *
     * @param parameters 参数集
     * @param message    原始日志
     * @param name       结果日志列名
     * @param fieldName  原始日志列名
     */
    private static void alignmentUtils(String parameters, JSONObject message, String name, String fieldName) {
        String[] alignmentPars = parameters.split(StreamAggregateConfig.FORMAT_SPLITTER);
        String data = message.getString(fieldName);

        int subscript = Integer.parseInt(alignmentPars[0]);
        String[] fieldSplit = data.split(alignmentPars[1]);
        Long appID = Long.valueOf(fieldSplit[subscript]);
        int length = fieldSplit[subscript].length();
        StringBuilder sb = new StringBuilder(data);
        message.put(name, sb.replace(0, length, appMap.get(appID)));
    }

    /**
     * combination 拼接操作
     *
     * @param parameters    参数集
     * @param message       原始日志
     * @param name          结果日志列名
     * @param fieldName     原始日志列名
     * @param dimensionsObj 结果集
     */
    private static void combinationUtils(String parameters, JSONObject message, String name, String fieldName, JSONObject dimensionsObj) {
        String[] combinationPars = parameters.split(StreamAggregateConfig.FORMAT_SPLITTER);
        String parameter0Value = message.getString(combinationPars[0]);
        if (StringUtil.isNotBlank(parameter0Value)) {
            String combinationValue = parameter0Value + combinationPars[1] + message.getString(fieldName);
            message.put(fieldName, combinationValue);
            dimensionsObj.put(name, combinationValue);
        }
    }
}
