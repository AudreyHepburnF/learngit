package cn.bidlink.job.purchase.handler;

import cn.bidlink.framework.redis.BidRedis;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Random;

@Service
@JobHander("purchaseQuantityJobHandler")
public class PurchaseQuantityJobHandler extends IJobHandler /*implements InitializingBean*/ {
    private Logger logger                = LoggerFactory.getLogger(PurchaseQuantityJobHandler.class);

    // 总交易量
    private String TOTAL_TRANSACTION_NUM = "total_transaction_num";
    // 今日交易量
    private String TODAY_TRANSACTION_NUM = "today_transaction_num";

    //每日交易额随机数
    @Value("${randomNum:500}")
    private Integer randomNum;

    @Value("${totalTransactionNum:300000}")
    private Long totalTransactionNum;

    @Value("${todayTransactionNum:1500}")
    private Integer todayTransactionNum;

    private Random random = new Random();
    @Autowired
    private BidRedis bidRedis;

    @Override
    /**
     *  每天采购交易量范围：1500 - 2000
     *  采购交易总额基数： 2310301230
     */
    public ReturnT<String> execute(String... strings) throws Exception {
        int todayTransactionNumToUse = calculateTodayTransactionNum();
        // 更新每日交易额
        updateTodayTransactionNum(todayTransactionNumToUse);
        // 更新总交易额
        updateTotalTransactionNum(todayTransactionNumToUse);
        return ReturnT.SUCCESS;
    }


    private void updateTotalTransactionNum(int todayTransactionNumToUse) {
        if (bidRedis.exists(TOTAL_TRANSACTION_NUM)) {
            Long totalTransactionNum = (Long) bidRedis.getObject(TOTAL_TRANSACTION_NUM);
            bidRedis.setObject(TOTAL_TRANSACTION_NUM, totalTransactionNum + todayTransactionNumToUse);
            logger.info("采购商项目总交易量：" + (totalTransactionNum + todayTransactionNumToUse));
        } else {
            bidRedis.setObject(TOTAL_TRANSACTION_NUM, totalTransactionNum);
            logger.info("采购商项目总交易量：" + (totalTransactionNum));
        }
    }

    private void updateTodayTransactionNum(int todayTransactionNumToUse) {
        bidRedis.setObject(TODAY_TRANSACTION_NUM, todayTransactionNumToUse);
        logger.info("采购商项目今日交易量：" + todayTransactionNumToUse);
    }

    private int calculateTodayTransactionNum() {
        return todayTransactionNum + random.nextInt(randomNum);
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
////        execute();
////        System.out.println(bidRedis.getObject(TOTAL_TRANSACTION_NUM));
////        System.out.println(bidRedis.getObject(TODAY_TRANSACTION_NUM));
//        Long totalTransactionNum = (Long) bidRedis.getObject(TOTAL_TRANSACTION_NUM);
//        System.out.println(totalTransactionNum);
//        Integer todayTransactionNum = (Integer) bidRedis.getObject(TODAY_TRANSACTION_NUM);
//        System.out.println(todayTransactionNum);
//    }
}
