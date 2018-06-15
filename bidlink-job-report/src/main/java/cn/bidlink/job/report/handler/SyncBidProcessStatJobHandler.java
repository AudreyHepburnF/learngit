package cn.bidlink.job.report.handler;

import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 招标进展情况报表
 *
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/12/25
 */
@Service
@JobHander("syncBidProcessStatJobHandler")
public class SyncBidProcessStatJobHandler extends SyncJobHandler /*implements InitializingBean*/ {
    private Logger logger = LoggerFactory.getLogger(SyncBidProcessStatJobHandler.class);

    @Override
    protected String getTableName() {
        return "bid_process_stat";
    }

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步招标进展情况报表统计开始");
        clearBidProcessStatusProcess();
        syncBidProcessStatusProcess();
        // 记录同步信息
        updateSyncLastTime();
        logger.info("同步招标进展情况报表统计结束");
        return ReturnT.SUCCESS;
    }

    private void clearBidProcessStatusProcess() {
        logger.info("清理招标进展情况报表统计开始");
        clearTableData();
        logger.info("清理招标进展情况报表统计结束");
    }

    /**
     * 同步采购进展情况
     */
    private void syncBidProcessStatusProcess() {
        // 获取上次同步时间
        Date lastSyncTime = getLastSyncTime();
        logger.info("同步招标进展情况报表统计lastSyncTime: " + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));
        String countSql = "SELECT\n"
                          + "   count(1)"
                          + "FROM\n"
                          + "   proj_inter_project\n";

        String querySql = "SELECT\n"
                          + "   project_status,\n"
                          + "   company_id,\n"
                          + "   project_amount_rmb,\n"
                          + "   create_time\n"
                          + "FROM\n"
                          + "   proj_inter_project\n"
                          + "limit ?,?";
        List<Object> params = new ArrayList<>();
//        params.add(lastSyncTime);
        sync(ycDataSource, countSql, querySql, params);
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
