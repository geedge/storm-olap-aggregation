package cn.ac.iie.storm.utils.combine;

import cn.ac.iie.storm.utils.file.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.tuple.Values;


import java.util.HashMap;

/**
 * @ClassNameAggregateUtils
 * @Author lixkvip@126.com
 * @Date2020/6/23 14:04
 * @Version V1.0
 **/
public class AggregateUtils {
    private final static Logger logger = Logger.getLogger(AggregateUtils.class);

    /**
     * Long类型的数据求和
     *
     * @param value1 第一个值
     * @param value2 第二个值
     * @return value1 + value2
     */
    public static Long longSum(String value1, String value2) {
        value1 = (value1 == null) ? "0" : value1;
        value2 = (value2 == null) ? "0" : value2;

        return Long.parseLong(value1) + Long.parseLong(value2);
    }

    /**
     * 计算Count
     *
     * @param count 当前count值
     * @return count+1
     */
    public static Long count(Long count) {

        count++;
        return count;
    }


    /**
     * 返回指标列的Map集合
     *
     * @param schema 动态获取的schema
     * @return 指标列集合
     * （c2s_byte_len, { "function" : "c2s_byte_sum", "name" : "c2s_byte_len", "fieldName" : "common_c2s_byte_num" }）
     */
    public static HashMap<String, JSONObject> getMetircsMap(String schema) {
        HashMap<String, JSONObject> metricsMap = new HashMap<>(16);

        JSONObject jsonObject = JSONObject.parseObject(schema);
        String data = jsonObject.getString("data");
        JSONArray metrics = JSONObject.parseArray(JSONObject.parseObject(JSONObject.parseObject(data).getString("doc")).getString("metrics"));

        for (Object metric : metrics) {
            JSONObject json = JSONObject.parseObject(metric.toString());
            String name = json.getString("name");
            metricsMap.put(name, json);
        }

        return metricsMap;
    }


    /**
     * 递归发送tuple
     *
     * @param headIndex    ssss
     * @param splitArr
     * @param initStr
     * @param collector
     * @param message
     * @param dimesionsObj
     * @param name
     */
    public static void reSend(int headIndex, String[] splitArr, String initStr, BasicOutputCollector collector, JSONObject message, JSONObject dimesionsObj, String name) {
        //递归拼接字符串
        if (splitArr.length != headIndex - 1) {
            //递归的核心代码
            if ("".equals(initStr)) {
                initStr = splitArr[splitArr.length - headIndex];
            } else {
                initStr = initStr + "/" + splitArr[splitArr.length - headIndex];
            }
            dimesionsObj.put(name, initStr);

            collector.emit(new Values(splitArr[splitArr.length - headIndex], dimesionsObj.toString(), message.toString()));
            reSend(headIndex + 1, splitArr, initStr, collector, message, dimesionsObj, name);
        }
    }


    /**
     * 获取action模块的Map集合
     *
     * @param schema 动态获取的schema
     * @return （HTTP，metrics数组）
     */
    public static HashMap<String, String[]> getActionMap(String schema) {
        JSONObject jsonObject = JSONObject.parseObject(schema);
        String data = jsonObject.getString("data");
        JSONArray actions = JSONObject.parseArray(JSONObject.parseObject(JSONObject.parseObject(data).getString("doc")).getString("action"));
        HashMap<String, String[]> map = new HashMap<>(16);

        for (Object action : actions) {
            JSONObject json = JSONObject.parseObject(action.toString());

            String label = json.getString("label");
            String[] metrics = json.getString("metrics").split(",");

            map.put(label, metrics);
        }
        return map;
    }


    /**
     * 获取时间列的集合
     *
     * @param schema 动态获取的schema
     * @return 时间列
     */
    public static String getTimeMetric(String schema) {
        JSONObject jsonObject = JSONObject.parseObject(schema);
        String data = jsonObject.getString("data");

        return JSONObject.parseObject(JSONObject.parseObject(JSONObject.parseObject(data)
                .getString("doc"))
                .getString("timestamp"))
                .getString("name");
    }

    public static void main(String[] args) {
        String sc = "{\"status\":200,\"code\":\"200666\",\"queryKey\":null,\"success\":true,\"message\":\"ok\",\"statistics\":null,\"formatType\":null,\"meta\":null,\"data\":{\"type\":\"record\",\"name\":\"liveChart\",\"doc\":{\"timestamp\":{\"name\":\"stat_time\",\"type\":\"Long\"},\"dimensions\":[{\"name\":\"protocol_id\",\"fieldName\":\"common_protocol_label\",\"type\":\"String\"},{\"name\":\"entrance_id\",\"fieldName\":\"common_entrance_id\",\"type\":\"String\"},{\"name\":\"isp\",\"fieldName\":\"common_isp\",\"type\":\"String\"},{\"name\":\"data_center\",\"fieldName\":\"common_data_center\",\"type\":\"String\"}],\"metrics\":[{\"function\":\"sum\",\"name\":\"sessions\",\"fieldName\":\"common_sessions\"},{\"function\":\"sum\",\"name\":\"c2s_byte_num\",\"fieldName\":\"common_c2s_byte_num\"},{\"function\":\"sum\",\"name\":\"s2c_byte_num\",\"fieldName\":\"common_s2c_byte_num\"},{\"function\":\"sum\",\"name\":\"c2s_pkt_num\",\"fieldName\":\"common_c2s_pkt_num\"},{\"function\":\"sum\",\"name\":\"s2c_pkt_num\",\"fieldName\":\"common_s2c_pkt_num\"},{\"function\":\"sum\",\"name\":\"c2s_ipfrag_num\",\"fieldName\":\"common_c2s_ipfrag_num\"},{\"function\":\"sum\",\"name\":\"s2c_ipfrag_num\",\"fieldName\":\"common_s2c_ipfrag_num\"},{\"function\":\"sum\",\"name\":\"c2s_tcp_lostlen\",\"fieldName\":\"common_c2s_tcp_lostlen\"},{\"function\":\"sum\",\"name\":\"s2c_tcp_lostlen\",\"fieldName\":\"common_s2c_tcp_lostlen\"},{\"function\":\"sum\",\"name\":\"c2s_tcp_unorder_num\",\"fieldName\":\"common_c2s_tcp_unorder_num\"},{\"function\":\"sum\",\"name\":\"s2c_tcp_unorder_num\",\"fieldName\":\"common_s2c_tcp_unorder_num\"},{\"function\":\"disCount\",\"name\":\"unique_sip_num\",\"fieldName\":\"common_server_ip\"},{\"function\":\"disCount\",\"name\":\"unique_cip_num\",\"fieldName\":\"common_client_ip\"}],\"filters\":{\"type\":\"and\",\"fields\":[{\"fieldName\":\"common_protocol_label\",\"type\":\"not\",\"values\":null}]},\"transforms\":[{\"fieldName\":\"common_app_label\",\"function\":\"replaceapp\",\"name\":\"common_app_label\",\"parameters\":\"0,/\"},{\"function\":\"combination\",\"name\":\"protocol_id\",\"fieldName\":\"common_protocol_label\",\"parameters\":\"common_app_label,/\"},{\"function\":\"hierarchy\",\"name\":\"protocol_id\",\"fieldName\":\"common_protocol_label\",\"parameters\":\"/\"}],\"action\":[{\"label\":\"Default\",\"metrics\":\"sessions,c2s_byte_num,s2c_byte_num,c2s_pkt_num,s2c_pkt_num,c2s_ipfrag_num,s2c_ipfrag_num,c2s_tcp_lostlen,s2c_tcp_lostlen,c2s_tcp_unorder_num,s2c_tcp_unorder_num\"}],\"granularity\":{\"type\":\"period\",\"period\":\"5M\"}},\"fields\":[],\"task\":\"Protocol-Distribution\",\"in\":\"CONNECTION-RECORD-COMPLETED-LOG\",\"out\":\"TRAFFIC-PROTOCOL-STAT-LOG\"}}";
        JSONObject jsonObject = JSONObject.parseObject(sc);
        String data = jsonObject.getString("data");

        System.out.println(JSONObject.parseObject(JSONObject.parseObject(JSONObject.parseObject(data)
                .getString("doc"))
                .getString("timestamp"))
                .getString("name"));
    }

    /**
     * 更新缓存中的对应关系map
     *
     * @param hashMap 当前缓存对应关系map
     */
    public static void updateAppRelation(HashMap<Long, String> hashMap) {
        try {
            Long begin = System.currentTimeMillis();
            String schema = HttpClientUtil.requestByGetMethod(StreamAggregateConfig.APP_ID_HTTP);
            String data = JSONObject.parseObject(schema).getString("data");
            JSONArray objects = JSONArray.parseArray(data);
            for (Object object : objects) {
                JSONArray jsonArray = JSONArray.parseArray(object.toString());
                Long key = jsonArray.getLong(0);
                String value = jsonArray.getString(1);
                if (hashMap.containsKey(key)) {
                    if (!value.equals(hashMap.get(key))) {
                        hashMap.put(key, value);
                    }
                } else {
                    hashMap.put(key, value);
                }

            }
            System.out.println((System.currentTimeMillis() - begin));
            logger.warn("更新缓存中的对应的APP关系,拉取接口数据长度:[" + objects.size());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("更新缓存APP-ID失败,异常：" + e);
        }
    }

    /**
     * 解析 dimensions 字段集
     *
     * @param dimensions 维度集
     * @param message    原始日志
     * @return 结果维度集
     */
    public static JSONObject transDimensions(JSONArray dimensions, JSONObject message) {
        JSONObject dimensionsObj = new JSONObject();
        for (Object dimension : dimensions) {
            String fieldName = JSONObject.parseObject(dimension.toString()).getString("fieldName");
            String name = JSONObject.parseObject(dimension.toString()).getString("name");
            dimensionsObj.put(name, message.get(fieldName));
        }
        return dimensionsObj;
    }
}
