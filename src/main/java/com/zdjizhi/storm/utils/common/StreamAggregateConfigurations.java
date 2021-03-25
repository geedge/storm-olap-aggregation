package com.zdjizhi.storm.utils.common;

import com.zdjizhi.storm.utils.exception.AggregationException;
import com.zdjizhi.utils.StringUtil;
import org.apache.commons.lang.ObjectUtils;

import java.io.IOException;
import java.util.Locale;
import java.util.Properties;


/**
 * @author Administrator
 */

public final class StreamAggregateConfigurations {

    private static Properties propService = new Properties();


    public static String getStringProperty(Integer type, String key) {
        if (type == 0) {
            return propService.getProperty(key);
        } else {
            return null;
        }

    }

    public static Integer getIntProperty(Integer type, String key) {
        if (type == 0) {
            return Integer.parseInt(propService.getProperty(key));
        } else {
            return null;
        }
    }

    public static Long getLongProperty(Integer type, String key) {
        if (type == 0) {
            return Long.parseLong(propService.getProperty(key));
        } else {
            return null;
        }
    }

    public static Boolean getBooleanProperty(Integer type, String key) {
        if (type == 0) {
            return StringUtil.equals(propService.getProperty(key).toLowerCase().trim().toUpperCase(Locale.ENGLISH), "true");
        } else {
            return null;
        }
    }

    static {
        try {
            propService.load(StreamAggregateConfigurations.class.getClassLoader().getResourceAsStream("service_flow_config.properties"));
        } catch (IOException | AggregationException e) {
            propService = null;
        }
    }
}
