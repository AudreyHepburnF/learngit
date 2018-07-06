package cn.bidlink.job.synergy.handler;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHander;
import net.sf.json.JSONObject;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:供应商合同失效通知
 * @Date 2018/7/4
 */
@Service
@JobHander("supplierContractInvalidJobHandler")
public class SupplierContractInvalidJobHandler extends IJobHandler implements InitializingBean {

    private Logger logger = LoggerFactory.getLogger(SupplierContractInvalidJobHandler.class);
    private Properties properties;

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        logger.info("1【开始】执行供应商合同失效任务");
        CloseableHttpClient client = null;
        CloseableHttpResponse response = null;
        String url = null;
        try {
            String ipStr = properties.getProperty("contract.message.new.ip");
            String[] ipArr = ipStr.split(",");
            Random random = new Random();
            String realIp = ipArr[random.nextInt(ipArr.length)];
            logger.info("执行供应商合同失效通知机器ip:{}", realIp);
            StringBuilder urlBuilder = new StringBuilder();
            String port = properties.getProperty("contract.message.new.port");
            url = urlBuilder.append(realIp).append(":").append(port).append("/businessMessage/sendInvalidMessage").toString();
            HttpGet request = new HttpGet(url);
            client = HttpClients.createDefault();
            logger.info("1.1【开始】调用合同失效cloud接口,url:{}", url);
            response = client.execute(request);
            logger.info("1.2【结束】调用合同失效cloud接口,url:{}", url);

            if (response != null && response.getStatusLine().getStatusCode() == 200) {
                String jsonResponse = EntityUtils.toString(response.getEntity(), "UTF-8");
                JSONObject json = JSONObject.fromObject(jsonResponse);
                boolean success = json.getBoolean("success");
                if (success) {
                    logger.info("1.3 执行供应商合同失效任务成功,url:{}", url);
                    return ReturnT.SUCCESS;
                } else {
                    logger.info("1.3 执行供应商合同任务失败,url:{}, 失败原因:{}", url, json.get("error"));
                }
            } else {
                logger.info("1.3 执行供应商合同失效任务调用cloud接口无响应, url:{}", url);
            }
            return ReturnT.FAIL;
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("1.4 执行供应商合同失效任务失败, url:{}, 异常:{}", url, e);
            return ReturnT.FAIL;
        } finally {
            logger.error("1.5 执行供应商合同失效任务关闭资源,释放连接, url:{}", url);
            try {
                if (client != null) {
                    client.close();
                }
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("1.6 执行供应商合同失效任务关闭资源,释放连接失败, url:{}, 异常:{}", url, e);
            }
            logger.info("2【结束】执行供应商合同失效任务");
        }

    }

    @Override
    public void afterPropertiesSet() throws Exception {
        initProperties();
//        properties = new Properties();
//        properties.put("contract.message.new.ip", "http://192.168.8.22");
//        properties.put("contract.message.new.port", "8047");
//        execute();
    }

    private void initProperties() {
        try {
            properties = PropertiesLoaderUtils.loadProperties(new ClassPathResource("purchase.properties"));
        } catch (IOException e) {
            throw new RuntimeException("消息配置文件初始化失败");
        }
    }

}
