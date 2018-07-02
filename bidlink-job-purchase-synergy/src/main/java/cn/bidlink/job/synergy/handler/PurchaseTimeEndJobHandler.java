package cn.bidlink.job.synergy.handler;

import cn.bidlink.framework.redis.BidRedis;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHander;
import net.sf.json.JSONObject;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
@JobHander("purchaseTimeEndJobHandler")
public class PurchaseTimeEndJobHandler extends IJobHandler {

    private Logger logger = LoggerFactory.getLogger(PurchaseTimeEndJobHandler.class);

    private Properties properties;

    @Autowired
    private BidRedis bidRedis;

    private String key="job-purchase_time_end_last_time";

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        CloseableHttpClient client = null;
        CloseableHttpResponse response = null;
        String ip="";
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            if(bidRedis.exists(key)){
                Date lastTime = (Date)bidRedis.getObject(key);
                properties = PropertiesLoaderUtils.loadProperties(new ClassPathResource("purchase.properties"));
                //调用同步的方法
                client = HttpClients.createDefault();
                ip = properties.getProperty("purchase.message.new.ip");
                String[] splits = ip.split(",");
                Random random = new Random();
                ip=splits[random.nextInt(splits.length)];
                String port = properties.getProperty("purchase.message.new.port");
                StringBuffer sb = new StringBuffer();
                String url = sb.append(ip).append(":").append(port).append("/message/sendTimeEndMessage").toString();
                HttpPost post = new HttpPost(url);
                List<NameValuePair> nvps = new ArrayList<NameValuePair>();
                String lastTimeStr = dateFormat.format(lastTime);
                nvps.add(new BasicNameValuePair("lastTime",lastTimeStr));
                Date nowTime=new Date();
                String nowTimeStr = dateFormat.format(nowTime);
                nvps.add(new BasicNameValuePair("nowTime",nowTimeStr));
                post.setEntity(new UrlEncodedFormEntity(nvps,"utf-8"));
                bidRedis.setObject(key,nowTime);
                logger.info("1.开始调用报价时间截止定时任务 ip:{} lastTime:{} nowTime:{}",ip,lastTimeStr,nowTimeStr);
                response = client.execute(post);
                logger.info("2.报价时间截止定时任务返回 ip:{}",ip);
                if (response != null && response.getStatusLine().getStatusCode() == 200) {
                    String jsonResponse = EntityUtils.toString(response.getEntity(), "UTF-8");
                    JSONObject json = JSONObject.fromObject(jsonResponse);
                    boolean status = json.getBoolean("success");
                    if (status) {
                        logger.info("3.报价时间截止定时任务成功 ip:{}",ip);
                    } else {
                        logger.error("3.报价时间截止定时任务失败 ip:{} ,原因为:{}",ip,json.get("error"));
                    }
                } else {
                    logger.info("3.报价时间截止定时任务无响应或响应失败 ip:{}",ip);
                }
            }else{
                Date nowTime=new Date();
                bidRedis.setObject(key,nowTime);
                logger.info("报价时间截止定时任务上次执行时间不存在，设置上次执行时间为:{}",dateFormat.format(nowTime));
            }
                return ReturnT.SUCCESS;
        } catch (Exception e) {
            logger.error("4.采购商：采购项目同步异常 ip:{}, 原因为：{}", ip,e);
            e.printStackTrace();
            return ReturnT.FAIL;
        } finally {
            // 关闭连接,释放资源
            logger.info("5.报价时间截止定时任务,关闭连接,释放资源 ip:{}",ip);
            try {
                if (client != null) {
                    client.close();
                }
                if (response != null) {
                    response.close();
                }
            } catch (Exception e) {
                logger.error("5.报价时间截止定时任务,关闭连接，释放资源发生错误。ip：{} 原因：{}", ip, e);
            }
        }
    }

    public static void main(String[] args) throws IOException {
       /* String ip = "1,2,3";
        String[] splits = ip.split(",");
        Random random = new Random();
        for (int i=0;i<10;i++){
            System.out.println(splits[random.nextInt(splits.length)]);
        }*/
      /*  CloseableHttpClient client = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        HttpPost post = new HttpPost("http://10.4.0.183:8047/message/sendTimeEndMessage");
        List<NameValuePair> nvps = new ArrayList<NameValuePair>();
        nvps.add(new BasicNameValuePair("lastTime",dateFormat.format(new Date())));
        nvps.add(new BasicNameValuePair("nowTime",dateFormat.format(new Date())));
        post.setEntity(new UrlEncodedFormEntity(nvps,"utf-8"));
        response = client.execute(post);
        if (response != null && response.getStatusLine().getStatusCode() == 200) {
            String jsonResponse = EntityUtils.toString(response.getEntity(), "UTF-8");
            System.out.println(jsonResponse);
        }
        response.close();
        client.close();*/
    }

}
