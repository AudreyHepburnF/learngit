package cn.bidlink.job.synergy.handler;

import cn.bidlink.framework.redis.BidRedis;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHander;
import net.sf.json.JSONObject;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;

/**
 * @author : chongmianhe@ebnew.com
 * @description :
 * @time : 2017/12/12 10:23
 */
@Service
@JobHander("purchaseSynergyJobHandler")
public class PurchaseSynergyJobHandler extends IJobHandler implements InitializingBean{

    private Logger logger = LoggerFactory.getLogger(PurchaseSynergyJobHandler.class);

    private String PURCHASE_LAST_SYNERGY_TIME = "purchase_last_synergy_time";
    @Autowired
    private BidRedis bidRedis;

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        try {
            if (bidRedis.exists(PURCHASE_LAST_SYNERGY_TIME)) {
                Date startTime = (Date) bidRedis.getObject(PURCHASE_LAST_SYNERGY_TIME);
                Date endTime = new Date();
                logger.info("1.采购商：开始同步采购项目。startTime:{},endTime:{}",startTime,endTime);
                //调用同步的方法
                HttpClient client = new DefaultHttpClient();
                HttpGet get = new HttpGet("http://10.4.0.28:8047/synPurchase?startTime="+startTime.getTime()+"&endTime="+endTime.getTime());
                HttpResponse response = client.execute(get);
                String jsonResponse = EntityUtils.toString(response.getEntity(), "UTF-8");

                if (response != null && response.getStatusLine().getStatusCode() == 200) {
                    logger.info("2.采购商：服务器正确响应，下面开始处理返回数据......");
                    JSONObject json = JSONObject.fromObject(jsonResponse);
                    boolean status = json.getBoolean("success");

                    if (status) {
                        //设置下次同步的时间
                        bidRedis.setObject(PURCHASE_LAST_SYNERGY_TIME, endTime);
                        logger.info("3.采购商：采购项目同步时间：endTime:{}",endTime);
                    } else {
                        //设置下次同步的时间
                        bidRedis.setObject(PURCHASE_LAST_SYNERGY_TIME, endTime);
                        logger.error("3.采购商：采购项目同步失败：startTime:{},endTime:{}",startTime,endTime);
                    }
                } else {
                    logger.info("2.采购商：服务器未响应或者响应失败");
                }
            } else {
                bidRedis.setObject(PURCHASE_LAST_SYNERGY_TIME, new Date());
                logger.info("1.采购商：采购项目同步开始时间：startTime{}",bidRedis.getObject(PURCHASE_LAST_SYNERGY_TIME));
            }
            return ReturnT.SUCCESS;
        } catch (Exception e) {
            logger.error("4.采购商：采购项目同步异常。当前时间为：time:{}",new Date());
            e.printStackTrace();
            return ReturnT.FAIL;
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
//        execute();
    }
}
