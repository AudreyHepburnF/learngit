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

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:同步采购成交总金额报表统计
 * @Date 2018/1/4
 */
@Service
@JobHander("syncPurchaseTradingVolumeStatJobHandler")
public class SyncPurchaseTradingVolumeStatJobHandler extends SyncJobHandler /*implements InitializingBean*/{

    private Logger logger = LoggerFactory.getLogger(SyncPurchaseTradingVolumeStatJobHandler.class);

    @Override
    protected String getTableName() {
        return "purchase_trading_volume_stat";
    }

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        // 当前时间和线程绑定
        SyncTimeUtil.setCurrentDate();

        logger.info("同步采购成交总金额报表统计开始");
        syncPurchaseTradingVolume();
        // 记录同步时间
        updateSyncLastTime();
        logger.info("同步采购成交总金额报表统计结束");
        return ReturnT.SUCCESS;
    }

    private void syncPurchaseTradingVolume() {

        Date lastSyncTime = getLastSyncTime();
        logger.info("同步采购成交总金额报表统计lastSyncTime: " + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));

        String countSql = "SELECT\n" +
                "\tCOUNT(1)\n" +
                "FROM\n" +
                "\tbmpfjz_project_ext bpe\n" +
                "LEFT JOIN bmpfjz_project bp ON bpe.id = bp.id\n" +
                "AND bpe.comp_id = bp.comp_id\n" +
                "WHERE\n" +
                "\tbp.project_status IN (8, 9) \n" +
                " AND bpe.publish_bid_result_time > ?\n";


        String querySql = "SELECT\n" +
                "\tbpe.publish_bid_result_time,\n" +
                "\tbpe.deal_total_price,\n" +
                "\tbp.comp_id AS company_id\n" +
                "FROM\n" +
                "\tbmpfjz_project_ext bpe\n" +
                "LEFT JOIN bmpfjz_project bp ON bpe.id = bp.id\n" +
                "AND bpe.comp_id = bp.comp_id\n" +
                "WHERE\n" +
                "\tbp.project_status IN (8, 9)\n" +
                "AND bpe.publish_bid_result_time > ?\n" +
                "\t LIMIT ?,? \n";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        sync(ycDataSource, countSql, querySql, params);


    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
