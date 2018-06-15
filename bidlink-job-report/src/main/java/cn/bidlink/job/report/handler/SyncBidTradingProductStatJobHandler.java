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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author <a href="mailto:Libingwang@ebnew.com">libingwang</a>
 * @version Ver 1.0
 * @description:同步招标成交采购品列表统计
 * @Date 2018/01/08
 */
@Service
@JobHander("syncBidTradingProductStatJobHandler")
public class SyncBidTradingProductStatJobHandler extends SyncJobHandler /*implements InitializingBean*/ {
    private Logger logger = LoggerFactory.getLogger(SyncBidTradingProductStatJobHandler.class);

    private String PRODUCT_ID = "product_id";
    private String COMPANY_ID = "company_id";
    private String ID = "id";
    private String CATALOG_ID = "catalog_id";

    @Override
    protected String getTableName() {
        return "bid_trading_product_stat";
    }

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        // 时间和线程绑定
        SyncTimeUtil.setCurrentDate();
        logger.info("同步招标成交采购品列表统计开始");
        syncBiddingTradingProduct();
        // 记录上次同步时间
        updateSyncLastTime();
        logger.info("同步招标成交采购品列表统计结束");
        return ReturnT.SUCCESS;
    }

    private void syncBiddingTradingProduct() {

        Date lastSyncTime = getLastSyncTime();
        logger.info("同步招标成交采购品报表统计lastSyncTime: " + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));

        String countSql = "SELECT\n" +
                "\tcount(1)\n" +
                "FROM\n" +
                "\tPROJ_INTER_PROJECT P\n" +
                "LEFT JOIN BID B ON B.PROJECT_ID = P.ID\n" +
                "AND b.COMPANY_ID = p.COMPANY_ID\n" +
                "LEFT JOIN BID_PRODUCT BP ON BP.BID_ID = B.ID\n" +
                "AND BP.COMPANY_ID = b.COMPANY_ID\n" +
                "WHERE\n" +
                "\tB.IS_BID_SUCCESS = 1\n" +
                "AND B.IS_ABANDON = 0\n" +
                "AND B.IS_WITHDRAWBID = 0\n" +
                "AND P.create_time > ?\n"+
                "AND BP.ID IS NOT NULL" ;

        String querySql = "SELECT\n" +
                "  BP.company_id,\n" +
                "\tP.ID AS project_id,\n" +
                "\tP.project_name,\n" +
                "\tB.bider_name,\n" +
                "\tBP.product_id,\n" +
                "\tBP.product_name,\n" +
                "\tBP.product_code,\n" +
                "\tBP.product_model,\n" +
                "\tBP.product_unitname,\n" +
                "\tBP.product_number,\t\n" +
                "\tBP.total_bid_price,\n" +
                "  B.is_bid_success,\n" +
                "  B.is_abandon,\n" +
                "  B.is_withdrawbid,\n" +
                "  BP.ID AS bid_productId,\n" +
                "  P.create_time\n" +
                "FROM\n" +
                "\tPROJ_INTER_PROJECT P\n" +
                "LEFT JOIN BID B ON B.PROJECT_ID = P.ID\n" +
                "AND b.COMPANY_ID = p.COMPANY_ID\n" +
                "LEFT JOIN BID_PRODUCT BP ON BP.BID_ID = B.ID\n" +
                "AND BP.COMPANY_ID = b.COMPANY_ID\n" +
                "WHERE\t  \n" +
                "\tB.IS_BID_SUCCESS = 1\n" +
                "AND B.IS_ABANDON = 0\n" +
                "AND B.IS_WITHDRAWBID = 0\n" +
                "AND BP.ID IS NOT NULL\n" +
                "AND P.create_time > ?\n"+
                "\t\tLIMIT ?,?\n";
        ArrayList<Object> params = new ArrayList<>();
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

                // 添加catalog_id
                appendCatalogId(mapList);
                logger.debug("执行querySql : {}, paramsToUse : {}，共{}条", querySql, paramsToUse, mapList.size());
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
     * 添加采购品目录id
     *
     * @param mapList
     */
    private void appendCatalogId(List<Map<String, Object>> mapList) {
        // 查找所有的product_id,company_id
        Set<Pair> productIdPairs = new HashSet<>();
        for (Map<String, Object> mapAttr : mapList) {
            productIdPairs.add(new Pair((Long) mapAttr.get(COMPANY_ID), Long.valueOf((String) mapAttr.get(PRODUCT_ID))));
        }

        // 从corp_directorys找catalog_id
        StringBuilder selectCatalogIdSqlBuilder = new StringBuilder();
        selectCatalogIdSqlBuilder.append("SELECT\n" +
                "\tid,\n" +
                "\tcompany_id,\n" +
                "\tcatalog_id \n" +
                "FROM\n" +
                "\tcorp_directorys \n" +
                "WHERE \n" );

        int conditionIndex = 0;
        for (Pair pair : productIdPairs) {
            if(conditionIndex > 0) {
                selectCatalogIdSqlBuilder.append(" or").append("(id = ").append(pair.supplierId)
                        .append(" AND company_id = ").append(pair.companyId+")");
            }else{
                selectCatalogIdSqlBuilder.append("(id = ").append(pair.supplierId)
                        .append(" AND company_id = ").append(pair.companyId+")");
            }
            conditionIndex++;
        }

        // 将查出来的数据封装成map
        final Map<String, Long> catalogIdMap = DBUtil.query(ycDataSource, selectCatalogIdSqlBuilder.toString(), null, new DBUtil.ResultSetCallback<Map<String, Long>>() {
            @Override
            public Map<String, Long> execute(ResultSet resultSet) throws SQLException {
                HashMap<String, Long> map = new HashMap<>();
                while (resultSet.next()) {
                    long productId = resultSet.getLong(ID);
                    long companyId = resultSet.getLong(COMPANY_ID);
                    long catalogId = resultSet.getLong(CATALOG_ID);
                    String key = companyId + "_" + productId;
                    map.put(key,catalogId);
                }
                return map;
            }
        });
        // 遍历maplist, set
        for (Map<String, Object> mapAttr : mapList) {
            String key = mapAttr.get(COMPANY_ID) + "_" + mapAttr.get(PRODUCT_ID);
            Long catalogId = catalogIdMap.get(key);
            if (catalogId != null) {
                mapAttr.put(CATALOG_ID, catalogId);
            } else {
                mapAttr.put(CATALOG_ID, null);
            }
        }

    }

   /* @Override
    public void afterPropertiesSet() throws Exception {
        execute();
    }*/
}
