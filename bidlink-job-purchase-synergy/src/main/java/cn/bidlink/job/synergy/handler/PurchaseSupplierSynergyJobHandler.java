package cn.bidlink.job.synergy.handler;

import cn.bidlink.framework.redis.BidRedis;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Properties;

/**
 * @author : chongmianhe@ebnew.com
 * @description :
 * @time : 2017/12/12 10:23
 */
@Service
@JobHander("purchaseSupplierSynergyJobHandler")
public class PurchaseSupplierSynergyJobHandler extends IJobHandler /*implements InitializingBean*/ {

    private Logger logger = LoggerFactory.getLogger(PurchaseSupplierSynergyJobHandler.class);

    private String PURCHASE_SUPPLIER_LAST_SYNERGY_TIME = "purchase_supplier_last_synergy_time";

    private Properties properties;

    @Autowired
    private BidRedis bidRedis;

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        CloseableHttpClient client = null;
        CloseableHttpResponse response = null;
        try {
            properties = PropertiesLoaderUtils.loadProperties(new ClassPathResource("purchase.properties"));
            if (bidRedis.exists(PURCHASE_SUPPLIER_LAST_SYNERGY_TIME)) {
                Date startTime = (Date) bidRedis.getObject(PURCHASE_SUPPLIER_LAST_SYNERGY_TIME);
                Date endTime = new Date();
                logger.info("1.供应商：开始同步采购项目。startTime:{},endTime:{}", startTime, endTime);
                //调用同步的方法
                client = HttpClients.createDefault();
                String ip = properties.getProperty("purchase.synergy.new.ip");
                String port = properties.getProperty("purchase.synergy.new.port");
                StringBuffer sb = new StringBuffer();
                String url = sb.append(ip).append(":").append(port).append("/synSupplier?startTime=").append(startTime.getTime()).append("&endTime=").append(endTime.getTime()).toString();
                HttpGet get = new HttpGet(url);
                response = client.execute(get);
                String jsonResponse = EntityUtils.toString(response.getEntity(), "UTF-8");

                if (response != null && response.getStatusLine().getStatusCode() == 200) {
                    logger.info("2.供应商：服务器正确响应，下面开始处理返回数据......");
                    JSONObject json = JSONObject.fromObject(jsonResponse);
                    boolean status = json.getBoolean("success");

                    if (status) {
                        //设置下次同步的时间
                        bidRedis.setObject(PURCHASE_SUPPLIER_LAST_SYNERGY_TIME, endTime);
                        logger.info("3.供应商：采购项目同步时间：endTime:{}", endTime);
                    } else {
                        //设置下次同步的时间
                        bidRedis.setObject(PURCHASE_SUPPLIER_LAST_SYNERGY_TIME, endTime);
                        logger.error("3.供应商：采购项目同步失败：startTime:{},endTime:{}", startTime, endTime);
                    }
                } else {
                    logger.info("2.供应商：服务器未响应或者响应失败");
                }
            } else {
                bidRedis.setObject(PURCHASE_SUPPLIER_LAST_SYNERGY_TIME, new Date());
                logger.info("1.供应商：采购项目同步开始时间：startTime{}", bidRedis.getObject(PURCHASE_SUPPLIER_LAST_SYNERGY_TIME));
            }
            return ReturnT.SUCCESS;
        } catch (Exception e) {
            logger.error("4.供应商：采购项目同步异常。当前时间为：time:{}", new Date());
            e.printStackTrace();
            return ReturnT.FAIL;
        } finally {
            // 关闭连接,释放资源
            logger.info("5.供应商：关闭连接,释放资源");
            try {
                if (client != null) {
                    client.close();
                }
                if (response != null) {
                    response.close();
                }
            } catch (Exception e) {
                logger.error("5.供应商：关闭连接，释放资源发生错误。", e);
            }
        }
    }

    /*@Override
    public void afterPropertiesSet() throws Exception {
//        execute();
    }*/
}
