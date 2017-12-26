package cn.bidlink.job.report.handler;

import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;

/**
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/12/25
 */
@Service
@JobHander("syncBidSupplierStatJobHandler")
public class SyncBidSupplierStatJobHandler extends SyncJobHandler {
    private Logger logger = LoggerFactory.getLogger(SyncBidSupplierStatJobHandler.class);

    @Autowired
    @Qualifier("ycDataSource")
    private DataSource ycDataSource;

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步报价供应商列表统计开始");
        syncBidSupplier();
        logger.info("同步报价供应商列表统计结束");
        return ReturnT.SUCCESS;
    }

    @Override
    protected String getTableName() {
        return "bidSupplierStat";
    }

    private void syncBidSupplier() {
        // 将同步时间戳存到redis?

    }
}
