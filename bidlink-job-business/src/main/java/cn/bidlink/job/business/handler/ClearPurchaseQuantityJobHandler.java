package cn.bidlink.job.business.handler;

import cn.bidlink.framework.redis.BidRedis;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 清理采购商交易量统计
 */
@Service
@JobHander("clearPurchaseQuantityJobHandler")
public class ClearPurchaseQuantityJobHandler extends IJobHandler /*implements InitializingBean*/ {
    private Logger logger                = LoggerFactory.getLogger(ClearPurchaseQuantityJobHandler.class);

    // 总交易量
    private String TOTAL_TRANSACTION_NUM = "total_transaction_num";
    // 今日交易量
    private String TODAY_TRANSACTION_NUM = "today_transaction_num";

    @Autowired
    private BidRedis bidRedis;

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        logger.info("清除采购商项目交易开始");
        bidRedis.del(TODAY_TRANSACTION_NUM);
        bidRedis.del(TOTAL_TRANSACTION_NUM);
        logger.info("清除采购商项目交易结束");
        return ReturnT.SUCCESS;
    }


}
