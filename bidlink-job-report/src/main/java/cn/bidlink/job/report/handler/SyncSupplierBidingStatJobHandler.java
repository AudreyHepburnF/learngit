package cn.bidlink.job.report.handler;

import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:jiacaisu@ebnew.com">jiacaisu</a>
 * @version Ver 1.0
 * @description 供应商招标中标统计
 * @Date 2018/1/4
 */
@Service
@JobHander("syncSupplierBidingStatJobHandler")
public class SyncSupplierBidingStatJobHandler extends SyncJobHandler/* implements InitializingBean*/ {

    private Logger logger = LoggerFactory.getLogger(SyncSupplierBidingStatJobHandler.class);

    @Override
    protected String getTableName() {
        return "supplier_biding_stat";
    }


    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        // 当前时间和线程绑定
        SyncTimeUtil.setCurrentDate();

        logger.info("同步供应商招标中标统计开始");
        //盘内供应商
        syncSupplierInner();
        //盘外供应商
        syncSupplierOuter();
        // 记录同步时间
        updateSyncLastTime();
        logger.info("同步供应商招标中标统计结束");
        return ReturnT.SUCCESS;
    }

    private void syncSupplierInner() {

        Date lastSyncTime = getLastSyncTime();
        logger.info("同步盘内供应商招标中标统计开始lastSyncTime: " + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));

        String countSql = "SELECT\n" +
                "\tCOUNT(DISTINCT B.BIDER_ID)\n" +
                "FROM\n" +
                "\tBID B\n" +
                "LEFT JOIN PROJ_INTER_PROJECT P ON P.ID = B.PROJECT_ID\n" +
                "WHERE\n" +
                "\tp.COMPANY_ID = b.COMPANY_ID\n" +
                "AND B.IS_BID_SUCCESS = 1\n" +
                "AND P.PROJECT_STATUS IN (9, 11, 12)\n" +
                "AND B.BIDER_ID IN (\n" +
                "\tSELECT\n" +
                "\t\tsupplier_id\n" +
                "\tFROM\n" +
                "\t\tbsm_company_supplier\n" +
                ")";


        String querySql = "SELECT\n" +
                "\tt.pid,\n" +
                "\tt.PROJECT_NAME,\n" +
                "\tt.SUPPLIER_NAME,\n" +
                "\tt.SUPPLIER_ID,\n" +
                "\tt.TOTAL_PRICE,\n" +
                "\tt2.bidCount,\n" +
                "\tt.bidOkCount,\n" +
                "t2.COMPANY_ID,\n" +
                "1 as supplier_type\n" +
                "FROM\n" +
                "\t(\n" +
                "\t\tSELECT\n" +
                "\t\t\tP.ID AS pid,\n" +
                "\t\t\tP.PROJECT_NAME,\n" +
                "\t\t\tB.BIDER_NAME AS SUPPLIER_NAME,\n" +
                "\t\t\tB.BIDER_ID AS SUPPLIER_ID,\n" +
                "\t\t\tSUM(B.BIDER_PRICE_UNE) AS TOTAL_PRICE,\n" +
                "\t\t\tCOUNT(b.ID) AS bidOkCount\n" +
                "\t\tFROM\n" +
                "\t\t\tBID B\n" +
                "\t\tLEFT JOIN PROJ_INTER_PROJECT P ON P.ID = B.PROJECT_ID\n" +
                "\t\tAND p.COMPANY_ID = b.COMPANY_ID\n" +
                "\t\tWHERE\n" +
                "\t\t\tB.IS_BID_SUCCESS = 1\n" +
                "\t\tAND B.IS_ABANDON = 0\n" +
                "\t\tAND B.IS_WITHDRAWBID = 0\n" +
                "\t\tAND P.PROJECT_STATUS IN (9, 11, 12)\n" +
                "\t\tGROUP BY\n" +
                "\t\t\tB.BIDER_ID\n" +
                "\t) t\n" +
                "LEFT JOIN (\n" +
                "\tSELECT\n" +
                "\t\tB.BIDER_ID AS pid,\n" +
                "\t\tCOUNT(p.id) AS bidCount,\n" +
                "\t\tP.CREATE_TIME,\n" +
                "\t\tb.COMPANY_ID\n" +
                "\tFROM\n" +
                "\t\tBID B\n" +
                "\tLEFT JOIN PROJ_INTER_PROJECT P ON P.ID = B.PROJECT_ID\n" +
                "\tAND p.COMPANY_ID = b.COMPANY_ID\n" +
                "\tGROUP BY\n" +
                "\t\tB.BIDER_ID\n" +
                ") t2 ON t.SUPPLIER_ID = t2.pid\n" +
                "\n" +
                "WHERE\n" +
                "\tt.SUPPLIER_ID IN (\n" +
                "\t\tSELECT\n" +
                "\t\t\tsupplier_id\n" +
                "\t\tFROM\n" +
                "\t\t\tbsm_company_supplier\n" +
                "\t)\n" +
                "GROUP BY\n" +
                "\tt.SUPPLIER_ID  ORDER BY t2.CREATE_TIME DESC " +
                "\t LIMIT ?,? \n";
        ArrayList<Object> params = new ArrayList<>();
//        params.add(lastSyncTime);
        sync(ycDataSource, countSql, querySql, params);
    }

    private void syncSupplierOuter() {
        Date lastSyncTime = getLastSyncTime();
        logger.info("同步盘外供应商招标中标统计开始lastSyncTime: " + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));

        String countSql = "SELECT\n" +
                "\tCOUNT(DISTINCT B.BIDER_ID)\n" +
                "FROM\n" +
                "\tBID B\n" +
                "LEFT JOIN PROJ_INTER_PROJECT P ON P.ID = B.PROJECT_ID\n" +
                "AND p.COMPANY_ID = b.COMPANY_ID\n" +
                "WHERE\n" +
                "\tB.IS_BID_SUCCESS = 1\n" +
                "AND P.PROJECT_STATUS IN (9, 11, 12)\n" +
                "AND B.BIDER_ID IN (\n" +
                "\tSELECT\n" +
                "\t\tsupplier_id\n" +
                "\tFROM\n" +
                "\t\tbsm_company_supplier_apply\n" +
                "\tWHERE\n" +
                "\t\tsupplier_status != 4\n" +
                ")";


        String querySql = "SELECT\n" +
                "\tt.pid,\n" +
                "\tt.PROJECT_NAME,\n" +
                "\tt.SUPPLIER_NAME,\n" +
                "\tt.SUPPLIER_ID,\n" +
                "\tt.TOTAL_PRICE,\n" +
                "\tt2.bidCount,\n" +
                "\tt.bidOkCount,\n" +
                "  t.COMPANY_ID,\n" +
                "  2 as supplier_type\n" +
                "FROM\n" +
                "\t(\n" +
                "\t\tSELECT\n" +
                "\t\t\tP.ID AS pid,\n" +
                "      B.COMPANY_ID,\n" +
                "\t\t\tP.PROJECT_NAME,\n" +
                "\t\t\tB.BIDER_NAME AS SUPPLIER_NAME,\n" +
                "\t\t\tB.BIDER_ID AS SUPPLIER_ID,\n" +
                "\t\t\tSUM(B.BIDER_PRICE_UNE) AS TOTAL_PRICE,\n" +
                "\t\t\tCOUNT(b.ID) AS bidOkCount\n" +
                "\t\tFROM\n" +
                "\t\t\tBID B\n" +
                "\t\tLEFT JOIN PROJ_INTER_PROJECT P ON P.ID = B.PROJECT_ID\n" +
                "\t\tAND p.COMPANY_ID = b.COMPANY_ID\n" +
                "\t\tWHERE\n" +
                "\t\t\tB.IS_BID_SUCCESS = 1\n" +
                "\t\tAND B.IS_ABANDON = 0\n" +
                "\t\tAND B.IS_WITHDRAWBID = 0\n" +
                "\t\tAND P.PROJECT_STATUS IN (9, 11, 12)\n" +
                "\t\tGROUP BY\n" +
                "\t\t\tB.BIDER_ID\n" +
                "\t) t\n" +
                "LEFT JOIN (\n" +
                "\tSELECT\n" +
                "\t\tB.BIDER_ID AS pid,\n" +
                "\t\tCOUNT(p.id) AS bidCount,\n" +
                "\t\tP.CREATE_TIME\n" +
                "\tFROM\n" +
                "\t\tBID B\n" +
                "\tLEFT JOIN PROJ_INTER_PROJECT P ON P.ID = B.PROJECT_ID\n" +
                "\tAND p.COMPANY_ID = b.COMPANY_ID\n" +
                "\tGROUP BY\n" +
                "\t\tB.BIDER_ID\n" +
                ") t2 ON t.SUPPLIER_ID = t2.pid\n" +
                "where t.SUPPLIER_ID IN (\n" +
                "\tSELECT\n" +
                "\t\tsupplier_id\n" +
                "\tFROM\n" +
                "\t\tbsm_company_supplier_apply\n" +
                "\tWHERE\n" +
                "\t\tsupplier_status != 4\n" +
                ")\n" +
                "GROUP BY\n" +
                "\tt.SUPPLIER_ID    ORDER BY t2.CREATE_TIME DESC  " +
                "\t LIMIT ?,? \n";
        ArrayList<Object> params = new ArrayList<>();
//        params.add(lastSyncTime);
        sync(ycDataSource, countSql, querySql, params);
    }




/*
    @Override
    public void afterPropertiesSet() throws Exception {
        execute();
    }
*/

    /*protected void sync(DataSource dataSource, String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                // 添加分页查询参数
                List<Object> paramsToUse = appendToParams(params, i);
                List<Map<String, Object>> mapList = DBUtil.query(dataSource, querySql, paramsToUse);

                logger.debug("执行querySql : {}, paramsToUse : {}，共{}条", querySql, paramsToUse, mapList.size());
                // 生成insert sql, 参数
                if (!CollectionUtils.isEmpty(mapList)) {
                    System.out.println(mapList);
                }
            }
        }
    }*/
}
