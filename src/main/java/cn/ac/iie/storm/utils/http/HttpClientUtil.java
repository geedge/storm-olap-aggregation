package cn.ac.iie.storm.utils.http;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * 获取网关schema的工具类
 *
 * @author qidaijie
 */
public class HttpClientUtil {

    /**
     * 请求网关获取schema
     * @param http 网关url
     * @return schema
     */
    public static String requestByGetMethod(String http) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        StringBuilder entityStringBuilder = null;
        try {
            HttpGet get = new HttpGet(http);
            try (CloseableHttpResponse httpResponse = httpClient.execute(get)) {
                HttpEntity entity = httpResponse.getEntity();
                entityStringBuilder = new StringBuilder();
                if (null != entity) {
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(httpResponse.getEntity().getContent(), "UTF-8"), 8 * 1024);
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
                        entityStringBuilder.append(line);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (httpClient != null) {
                    httpClient.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return entityStringBuilder.toString();
    }

}
