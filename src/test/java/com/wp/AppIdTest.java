package com.wp;

import cn.ac.iie.storm.utils.file.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * @author qidaijie
 * @Package com.wp
 * @Description:
 * @date 2020/9/2215:09
 */
public class AppIdTest {

    @Test
    public void appTest() {
        //http://192.168.44.12:9999/open-api/appDicList
        String schema = HttpClientUtil.requestByGetMethod(StreamAggregateConfig.APP_ID_HTTP);
        JSONObject jsonObject = JSONObject.parseObject(schema);
        String data = jsonObject.getString("data");
        HashMap<Long, String> map = new HashMap<>(16);
        JSONArray objects = JSONArray.parseArray(data);
        for (Object object : objects) {
            JSONArray jsonArray = JSONArray.parseArray(object.toString());
            map.put(jsonArray.getLong(0), jsonArray.getString(1));
//            System.out.println(object);
        }
        System.out.println(map.toString());

        System.out.println(map.size());
    }

    @Test
    public void changeApp() {
        String a = "QQ";
        String[] alignmentPars = "0,/".split(StreamAggregateConfig.FORMAT_SPLITTER);
        String data = "100/HTTP";
        int subscript = Integer.parseInt(alignmentPars[0]);
        String[] fieldSplit = data.split(alignmentPars[1]);
        Long appID = Long.valueOf(fieldSplit[subscript]);
        int length = fieldSplit[subscript].length();
        StringBuilder sb = new StringBuilder(data);

        System.out.println(sb.replace(0, length, a));


    }
}
