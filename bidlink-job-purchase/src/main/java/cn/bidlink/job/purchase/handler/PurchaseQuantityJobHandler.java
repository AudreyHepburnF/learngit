package cn.bidlink.job.purchase.handler;

import cn.bidlink.framework.redis.BidRedis;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHander;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Date;

@Service
@JobHander
public class PurchaseQuantityJobHandler extends IJobHandler{

    @Autowired
    private BidRedis bidRedis;
    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        // 计算过程
        // 1500 - 2000
        // 基数 2310301230
        //今日交易量
        int today_transaction_num=0;
        //总的交易量
        int total_transaction_num=300000;
        //产生0-99随机数
        int x=(int)(Math.random()*10);
        //今日交易量
        if(bidRedis.exists("today_transaction_num")){
            SimpleDateFormat sdf =new SimpleDateFormat("HH:mm:ss");
            String time=sdf.format(new Date());
            String[] s=time.split(":");
            String hh=s[0];
            String min=s[1];
            //当小时和分钟大于23:50的时候 重新计算
            if(Integer.valueOf(hh)==23&&Integer.valueOf(min)>50){
                bidRedis.del("today_transaction_num");
            }else{
                bidRedis.setObject("today_transaction_num",(Integer.valueOf(bidRedis.getObject("today_transaction_num").toString())+x));
            }
        }else{
            bidRedis.setObject("today_transaction_num",(today_transaction_num+x));
        }

        //总的交易量
        if(bidRedis.exists("total_transaction_num")){
            bidRedis.setObject("total_transaction_num",(Integer.valueOf(bidRedis.getObject("total_transaction_num").toString())+x));
        }else{
            bidRedis.setObject("total_transaction_num",(total_transaction_num+x));
        }
        return ReturnT.SUCCESS;
    }
}
