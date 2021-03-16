package cn.ac.iie.storm.utils.http;

import cn.ac.iie.storm.utils.logout.LogPrintUtil;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * 获取网关schema的工具类
 *
 * @author qidaijie
 */
public class HttpClientUtil {

    private static Logger logger = Logger.getLogger(HttpClientUtil.class);


    /**
     * 请求网关获取schema
     *
     * @param http 网关url
     * @return schema
     */
    public static String requestByGetMethod(String http) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        StringBuilder entityStringBuilder = null;

        HttpGet get = new HttpGet(http);
        BufferedReader bufferedReader = null;
        try (CloseableHttpResponse httpResponse = httpClient.execute(get)) {
            HttpEntity entity = httpResponse.getEntity();
            entityStringBuilder = new StringBuilder();
            if (null != entity) {
                bufferedReader = new BufferedReader(new InputStreamReader(httpResponse.getEntity().getContent(), "UTF-8"), 8 * 1024);
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    entityStringBuilder.append(line);
                }
            }
        } catch (Exception e) {
            logger.error(LogPrintUtil.print(e));
        } finally {
            if (httpClient != null) {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    logger.error(LogPrintUtil.print(e));
                }
            }
            if (bufferedReader != null) {
//                    bufferedReader.close();
                org.apache.commons.io.IOUtils.closeQuietly(bufferedReader);
            }
        }
        return entityStringBuilder.toString();
    }

}
