package com.wp;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * @ClassNameFilterBolt
 * @Author lixkvip@126.com
 * @Date2020/7/1 14:53
 * @Version V1.0
 **/
public class FilterBolt {
    @SuppressWarnings("all")
    public static void main(String[] args) {
        JSONObject source = new JSONObject();


        String schema = "{\n" +
                "  \"task\": \"Application-Protocol-Distribution\",\n" +
                "  \"in\": \"CONNECTION-SKETCH-COMPLETED\",\n" +
                "  \"out\": \"TRAFFIC-PROTOCOL-STAT-LOG\",\n" +
                "  \"data\": {\n" +
                "    \"timestamp\": {\n" +
                "      \"name\": \"stat_time\"\n" +
                "    },\n" +
                "    \"dimensions\": [\n" +
                "      {\n" +
                "        \"name\": \"protocol_id\",\n" +
                "        \"fieldName\": \"common_protocol_label\",\n" +
                "        \"type\": \"String\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"device_id\",\n" +
                "        \"fieldName\": \"common_device_id\",\n" +
                "        \"type\": \"String\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"isp\",\n" +
                "        \"fieldName\": \"common_isp\",\n" +
                "        \"type\": \"String\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"metrics\": [\n" +
                "      { \"function\" : \"sessions_count\", \"name\" : \"sessions\"},\n" +
                "      { \"function\" : \"c2s_byte_sum\", \"name\" : \"c2s_byte_len\", \"fieldName\" : \"common_c2s_byte_num\" },\n" +
                "      { \"function\" : \"s2c_byte_sum\", \"name\" : \"s2c_byte_len\", \"fieldName\" : \"common_s2c_byte_num\" },\n" +
                "      { \"function\" : \"c2s_pkt_sum\", \"name\" : \"c2s_pkt_num\", \"fieldName\" : \"common_c2s_pkt_num\" },\n" +
                "      { \"function\" : \"s2c_pkt_sum\", \"name\" : \"s2c_pkt_num\", \"fieldName\" : \"common_s2c_pkt_num\" },\n" +
                "      { \"function\" : \"sip_disCount\", \"name\" : \"unique_sip_num\", \"fieldName\" : \"common_server_ip\" },\n" +
                "      { \"function\" : \"cip_disCount\", \"name\" : \"unique_cip_num\", \"fieldName\" : \"common_client_ip\" }\n" +
                "    ],\n" +
                "    \"filters\": {\n" +
                "      \"type\": \"and\",\n" +
                "      \"fields\": [\n" +
                "        {\n" +
                "          \"fieldName\": \"common_device_id\",\n" +
                "          \"type\": \"not\",\n" +
                "          \"values\": null\n" +
                "        },\n" +
                "        {\n" +
                "          \"fieldName\": \"common_protocol_label\",\n" +
                "          \"type\": \"not\",\n" +
                "          \"values\": null\n" +
                "        },\n" +
                "        {\n" +
                "          \"fieldName\": \"common_isp\",\n" +
                "          \"type\": \"not\",\n" +
                "          \"values\": null\n" +
                "        }\n" +
                "      ]\n" +
                "    },\n" +
                "    \"transforms\":[\n" +
                "      {\"function\":\"combination\",\"name\":\"protocol_id\",\"fieldName\":\"common_protocol_label\",\"parameters\": \"common_app_label,/\"},\n" +
                "      {\"function\":\"hierarchy\",\"name\":\"protocol_id\",\"fieldName\":\"common_protocol_label\",\"parameters\": \"/\"}\n" +
                "    ],\n" +
                "    \"action\":[\n" +
                "      {\"label\": \"Default\", \"metrics\": \"sessions,c2s_byte_len,s2c_byte_len,c2s_pkt_num,s2c_pkt_num\"},\n" +
                "      {\"label\": \"HTTP\", \"metrics\": \"sessions,c2s_byte_len,s2c_byte_len,c2s_pkt_num,s2c_pkt_num,unique_sip_num,unique_cip_num\"}\n" +
                "    ],\n" +
                "    \"granularity\":{\n" +
                "      \"type\": \"period\",\n" +
                "      \"period\": \"5M\"\n" +
                "    }\n" +
                "  }\n" +
                "}";


        source.put("common_protocol_label", "HTTP");
        source.put("common_isp", "Unicom");
        source.put("common_device_id", "1");
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
            }

            if (flag){
                System.out.println("输出到下个Bolt");
            }else {

                System.out.println("此条消息被过滤掉");
            }

        }
    }
}
