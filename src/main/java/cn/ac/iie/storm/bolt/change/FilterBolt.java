package cn.ac.iie.storm.bolt.change;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * @ClassNameFilterBolt
 * @Author lixkvip@126.com
 * @Date2020/7/1 12:02
 * @Version V1.0
 **/
public class FilterBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        JSONObject source = JSONObject.parseObject(input.getStringByField("source"));
        String schema = "";

        String data = JSONObject.parseObject(schema).getString("data");

        String filters = JSONObject.parseObject(data).getString("filters");

        boolean flag = true;
        String type = JSONObject.parseObject(filters).getString("type");
        JSONArray fieldsArr = JSONObject.parseArray(JSONObject.parseObject(filters).getString("fields"));
        if ("and".equals(type)) {
            for (int i = 0; i < fieldsArr.size(); i++) {

                JSONObject field = JSONObject.parseObject(fieldsArr.get(i).toString());
                String name = field.getString("fieldName");
                String fieldType = field.getString("type");
                Object values = field.get("values");

                Object nameValue = source.get(name);

                System.out.println("nameValue ========" +nameValue);

                if ("not".equals(fieldType)) {

                    if (nameValue == values) {
                        //满足过滤条件
                        flag = false;
                    }

                } else if ("in".equals(fieldType)) {
                    if (!values.toString().contains(nameValue.toString())) {
                        //满足过滤条件
                        flag = false;
                    }
                }
            }}



        collector.emit(new Values(source));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("filter"));
    }
}
