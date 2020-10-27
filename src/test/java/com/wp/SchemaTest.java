package com.wp;

import com.alibaba.fastjson.JSONObject;
import com.zdjizhi.utils.StringUtil;

/**
 * @ClassNameSchemaTest
 * @Author lixkvip@126.com
 * @Date2020/6/28 10:11
 * @Version V1.0
 **/
public class SchemaTest {

    static String str = "";

    public static void main(String[] args) {


        String str1 = null;
        String str2 = " ";


        System.out.println(StringUtil.isNotBlank(str1));
        System.out.println(StringUtil.isNotEmpty(str1));

        System.out.println(StringUtil.isNotBlank(str2));
        System.out.println(StringUtil.isNotEmpty(str2));

    }

    public static void reAdd(int m, String[] split, String str) {

        //递归拼接字符串
        if (0 == m) {
        } else {
            //递归的核心代码
            str = str + split[m - 1] + "/";
            reAdd(m - 1, split, str);

        }

    }
}
