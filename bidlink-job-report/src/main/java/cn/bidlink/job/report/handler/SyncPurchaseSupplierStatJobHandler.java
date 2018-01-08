package cn.bidlink.job.report.handler;

import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/12/25
 */
@Service
@JobHander("syncPurchaseSupplierStatJobHandler")
public class SyncPurchaseSupplierStatJobHandler extends SyncJobHandler /*implements InitializingBean*/ {

    private Logger logger = LoggerFactory.getLogger(SyncPurchaseSupplierStatJobHandler.class);


    private String SUPPLIER_ID = "supplier_id";
    private String COMPANY_ID = "company_id";
    private String SUPPLIER_STATUS = "supplier_status";

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步报价供应商列表统计开始");
        syncBidSupplierStat();
        //记录同步时间
        updateSyncLastTime();
        logger.info("同步报价供应商列表统计结束");
        return ReturnT.SUCCESS;
    }

    @Override
    protected String getTableName() {
        return "purchase_supplier_stat";
    }

    private void syncBidSupplierStat() {
        // 获取上次同步时间
        Date lastSyncTime = getLastSyncTime();
        logger.info("同步报价供应商lastSyncTime：" + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));
        String countSql = "SELECT\n" +
                "\tCOUNT(1)\n" +
                "FROM\n" +
                "\t(\n" +
                "\t\tSELECT\n" +
                "\t\t\tbpe.comp_id AS company_id,\n" +
                "\t\t\tbp.id\n" +
                "\t\tFROM\n" +
                "\t\t\tbmpfjz_project_ext bpe\n" +
                "\t\tINNER JOIN bmpfjz_project bp ON bpe.id = bp.id\n" +
                "\t\tAND bpe.comp_id = bp.comp_id\n" +
                "\t\tWHERE\n" +
                "\t\t\tbp.PROJECT_STATUS IN (8, 9)\n" +
                "\t\t\tAND bpe.publish_bid_result_time > ?\n" +
                "\t) s\n" +
                "INNER JOIN bmpfjz_supplier_project_bid bspb ON s.company_id = bspb.comp_id\n" +
                "AND s.id = bspb.project_id\n" +
                "WHERE\n" +
                "\tbspb.supplier_bid_status = 6";


        String querySql = "SELECT\n" +
                "\ts.company_id,\n" +
                "\tbspb.supplier_id,\n" +
                "\tbspb.supplier_name,\n" +
                "\tbspb.deal_total_price,\n" +
                "\ts.department_code,\n" +
                "\ts.publish_bid_result_time\n" +
                "FROM\n" +
                "\t(\n" +
                "\t\tSELECT\n" +
                "\t\t\tbpe.comp_id AS company_id,\n" +
                "\t\t\tbpe.publish_bid_result_time,\n" +
                "\t\t\tbp.id,\n" +
                "\t\t\tbp.department_code\n" +
                "\t\tFROM\n" +
                "\t\t\tbmpfjz_project_ext bpe\n" +
                "\t\tINNER JOIN bmpfjz_project bp ON bpe.id = bp.id\n" +
                "\t\tAND bpe.comp_id = bp.comp_id\n" +
                "\t\tWHERE bp.PROJECT_STATUS IN (8, 9)\n" +
                "\t\t\tAND bpe.publish_bid_result_time > ?\n" +
                "\t\tLIMIT ?,?\n" +
                "\t) s\n" +
                "INNER JOIN bmpfjz_supplier_project_bid bspb ON s.company_id = bspb.comp_id\n" +
                "AND s.id = bspb.project_id\n" +
                "WHERE bspb.supplier_bid_status = 6";
        List<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        sync(ycDataSource, countSql, querySql, params);


    }

    @Override
    protected void sync(DataSource dataSource, String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                // 添加分页查询参数
                List<Object> paramsToUse = appendToParams(params, i);
                List<Map<String, Object>> mapList = DBUtil.query(dataSource, querySql, paramsToUse);

                logger.debug("执行querySql : {}, paramsToUse : {}，共{}条", querySql, paramsToUse, mapList.size());

                // 添加是否是合作供应商
                appendSupplierStatus(mapList);

                // 生成insert sql, 参数
                if (!CollectionUtils.isEmpty(mapList)) {
                    StringBuilder sqlBuilder = sqlBuilder(mapList);
                    List<Object> insertParams = buildParams(mapList, sqlBuilder);
                    DBUtil.execute(reportDataSource, sqlBuilder.toString(), insertParams);
                }
            }
        }
    }

    /**
     * 添加供应商类型
     *
     * @param mapList
     */
    private void appendSupplierStatus(List<Map<String, Object>> mapList) {

        HashMap<String, Object> map = new HashMap<>();
        for (Map<String, Object> mapAttr : mapList) {
            Object supplierId = mapAttr.get(SUPPLIER_ID);
            Object companyId = mapAttr.get(COMPANY_ID);
            String key = supplierId + "_" + companyId;
            map.put(key, companyId);
        }

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("select supplier_id,company_id,supplier_status from bsm_company_supplier  where ");
        int conditionIndex = 0;
        for (Map.Entry<String,Object> attr : map.entrySet()) {
            String[] columns = attr.getKey().split("_");
            String supplierId = columns[0];
            String companyId = columns[1];
            if (conditionIndex > 0) {
                sqlBuilder.append(" or ").append("(supplier_id = " +supplierId  + " and company_id = " + companyId+ ")");
            } else {
                sqlBuilder.append("(supplier_id = " + supplierId + " and  company_id = " + companyId + ")");
            }
            conditionIndex++;
        }

        Map<Object, Object> supplierStatusMap = DBUtil.query(ycDataSource, sqlBuilder.toString(), null, new DBUtil.ResultSetCallback<Map<Object, Object>>() {
            @Override
            public Map<Object, Object> execute(ResultSet resultSet) throws SQLException {
                HashMap<Object, Object> map = new HashMap<>();
                while (resultSet.next()) {
                    long companyId = resultSet.getLong(COMPANY_ID);
                    long supplierId = resultSet.getLong(SUPPLIER_ID);
                    String key = supplierId + "_" + companyId;
                    map.put(key, resultSet.getObject(SUPPLIER_STATUS));
                }
                return map;
            }
        });

        for (Map<String, Object> mapAttr : mapList) {
            Object supplierId = mapAttr.get(SUPPLIER_ID);
            Object companyId = mapAttr.get(COMPANY_ID);
            String key = supplierId + "_" + companyId;
            mapAttr.put(SUPPLIER_STATUS, supplierStatusMap.get(key));
        }

    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
