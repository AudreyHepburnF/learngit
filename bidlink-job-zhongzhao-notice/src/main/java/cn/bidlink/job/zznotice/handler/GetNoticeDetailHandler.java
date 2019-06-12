package cn.bidlink.job.zznotice.handler;

import cn.bidlink.ctpsp.cloud.service.ZzNoticeService;
import cn.bidlink.ctpsp.dal.entity.ZzNotice;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHander;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.stereotype.Service;
import net.sf.json.JSONObject;

import java.util.*;

/**
 * Created by Administrator on 2019/6/4.
 */
@JobHander("getNoticeDetailHandler")
@Service
public class GetNoticeDetailHandler extends IJobHandler {
    private Logger logger = LoggerFactory.getLogger(GetNoticeDetailHandler.class);

    private Properties properties;

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        CloseableHttpClient client = null;
        CloseableHttpResponse response = null;
        String ip="";
        try {
            properties = PropertiesLoaderUtils.loadProperties(new ClassPathResource("notice.properties"));
            client = HttpClients.createDefault();
            ip = properties.getProperty("notice.message.new.ip");

            String port = properties.getProperty("notice.message.new.port");
            StringBuffer sb = new StringBuffer();
            String url = sb.append(ip).append(":").append(port).append("/doJob").toString();
            HttpPost post = new HttpPost(url);
            logger.info("1.开始调用发布公告定时任务 ip:{},nowTime:{}",ip);
            response = client.execute(post);

            if (response != null && response.getStatusLine().getStatusCode() == 200) {
                String jsonResponse = EntityUtils.toString(response.getEntity(), "UTF-8");
                JSONObject json = JSONObject.fromObject(jsonResponse);
                boolean status = json.getBoolean("success");
                if (status) {
                    logger.info("3.发布公告定时任务成功 ip:{}",ip);
                } else {
                    logger.error("3.发布公告定时任务失败 ip:{} ,原因为:{}",ip,json.get("error"));
                }
            } else {
                logger.info("3.发布公告定时任务无响应或响应失败 ip:{}",ip);
            }

            return ReturnT.SUCCESS;
        }catch (Exception e){
            logger.error("4.发布公告定时任务异常 ip:{}, 原因为：{}", ip,e);
            e.printStackTrace();
            return ReturnT.FAIL;
        }finally {
            // 关闭连接,释放资源
            logger.info("4.发布公告定时任务,关闭连接,释放资源 ip:{}",ip);
            try {
                if (client != null) {
                    client.close();
                }
                if (response != null) {
                    response.close();
                }
            } catch (Exception e) {
                logger.error("5.发布公告定时任务,关闭连接，释放资源发生错误。ip：{} 原因：{}", ip, e);
            }
        }
    }

    public static void main(String[] args) {
        GetNoticeDetailHandler job = new GetNoticeDetailHandler();
        try {
            ReturnT<String> execute = job.execute();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
