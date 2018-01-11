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
 * @description:同步采购成交采购品报表统计
 * @Date 2018/1/9
 */
@Service
@JobHander("syncPurchaseTradingProductStatJobHandler")
public class SyncPurchaseTradingProductStatJobHandler extends SyncJobHandler /*implements InitializingBean*/ {
    private Logger logger = LoggerFactory.getLogger(SyncPurchaseTradingProductStatJobHandler.class);

    @Override
    protected String getTableName() {
        return "purchase_trading_product_stat";
    }

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("采购成交采购品报表统计开始");
        syncPurchaseDealItem();
        updateSyncLastTime();
        logger.info("采购成交采购品报表统计结束");
        return ReturnT.SUCCESS;
    }

    private void syncPurchaseDealItem() {
        Date lastSyncTime = getLastSyncTime();
        logger.info("采购成交采购品报表统计 lastSyncTime : " + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));
        String countSql = "SELECT\n" +
                "\tCOUNT(1)\n" +
                "FROM\n" +
                "\tbmpfjz_supplier_project_item_bid bspib\n" +
                "INNER JOIN bmpfjz_project bp ON bspib.comp_id = bp.comp_id\n" +
                "AND bspib.project_id = bp.id\n" +
                "INNER JOIN CORP_DIRECTORYS cd ON bspib.directory_id = cd.ID\n" +
                "AND bp.comp_id = cd.company_id\n" +
                "WHERE\n" +
                "\tbp.project_status IN (8, 9)\n" +
                "AND bspib.bid_status = 3\n" +
                "AND cd.ABANDON = 1\n" +
                "AND bp.create_time > ?";
        String querySql = "SELECT\n" +
                "\tbspib.deal_price,\n" +
                "\tbspib.deal_total_price,\n" +
                "\tbspib.directory_id,\n" +
                "\tbp.id AS project_id,\n" +
                "\tbp.comp_id AS company_id,\n" +
                "\tcd.`NAME` AS item_name,\n" +
                "\tcd.`CODE` AS item_code,\n" +
                "\tcd.UNITNAME AS item_unit,\n" +
                "\tcd.tech_parameters AS item_params,\n" +
                "\tcd.SPEC AS item_spec,\n" +
                "\tcd.CATALOG_ID,\n" +
                "\tbp.create_time,\n" +
                "\tbspib.win_bid_time\n" +
                "FROM\n" +
                "\tbmpfjz_supplier_project_item_bid bspib\n" +
                "INNER JOIN bmpfjz_project bp ON bspib.comp_id = bp.comp_id\n" +
                "AND bspib.project_id = bp.id\n" +
                "INNER JOIN CORP_DIRECTORYS cd ON bspib.directory_id = cd.ID\n" +
                "AND bp.comp_id = cd.company_id\n" +
                "WHERE\n" +
                "\tbp.project_status IN (8, 9)\n" +
                "AND bspib.bid_status = 3\n" +
                "AND cd.ABANDON = 1\n" +
                "AND bp.create_time > ?\n" +
                "ORDER BY bspib.id\n" +
                "LIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        sync(ycDataSource, countSql, querySql, params);
    }


//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
