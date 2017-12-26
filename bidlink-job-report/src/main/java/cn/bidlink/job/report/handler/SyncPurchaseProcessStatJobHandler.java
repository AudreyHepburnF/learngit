package cn.bidlink.job.report.handler;

import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:同步采购流程项目概况列表统计
 * @Date 2017/12/26
 */
@Service
@JobHander("syncPurchaseProcessStatJobHandler")
public class SyncPurchaseProcessStatJobHandler extends SyncJobHandler {


    private Logger logger = LoggerFactory.getLogger(SyncPurchaseProcessStatJobHandler.class);
    /**
     * 表名
     *
     * @return
     */
    @Override
    protected String getTableName() {
        return "purchase_process_stat";
    }

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        // 时间和线程绑定
        SyncTimeUtil.setCurrentDate();
        logger.info("同步采购流程项目概况列表统计开始");
        syncPurchaseProcess();
        logger.info("同步采购流程项目概况列表统计结束");
        return ReturnT.SUCCESS;
    }

    /**
     * 同步采购流程项目概况列表统计
     *
     */
    private void syncPurchaseProcess() {
        // 获取上次同步时间

    }
}
