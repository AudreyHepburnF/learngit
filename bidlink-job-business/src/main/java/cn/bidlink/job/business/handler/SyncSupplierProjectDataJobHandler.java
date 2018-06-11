package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:同步供应商项目数据(成交项目 交易额等) {@link SyncSupplierDataJobHandler 供应商基本数据}
 * @Date 2018/5/24
 */
@Service
@JobHander(value = "syncSupplierProjectDataJobHandler")
public class SyncSupplierProjectDataJobHandler extends AbstractSyncSupplierDataJobHandler /*implements InitializingBean*/ {

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("开始同步供应商项目成交金额和数量");
        syncSupplierProjectData();
        logger.info("结束同步供应商项目成交金额和数量");
        return ReturnT.SUCCESS;
    }

    private void syncSupplierProjectData() {
        logger.info("同步供应商参与的项目统计开始");
        Properties properties = elasticClient.getProperties();
        int pageSizeToUse = 2 * pageSize;
        SearchResponse scrollResp = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.index"))
                .setTypes(properties.getProperty("cluster.type.supplier"))
                .setScroll(new TimeValue(60000))
                .setSize(pageSizeToUse)
                .get();

        int pageNumberToUse = 0;
        do {
            SearchHit[] searchHits = scrollResp.getHits().hits();
            Set<String> supplierIds = new HashSet<>();
            List<Map<String, Object>> resultFromEs = new ArrayList<>();
            for (SearchHit searchHit : searchHits) {
                supplierIds.add(searchHit.getId());
                resultFromEs.add(searchHit.getSource());
            }

            String supplierIdToString = StringUtils.collectionToCommaDelimitedString(supplierIds);
            // 添加已参与项目统计
            appendSupplierProjectStat(resultFromEs, supplierIdToString);
            // 添加成交项目统计
            appendSupplierDealProjectStat(resultFromEs, supplierIdToString);
            //  添加成交额统计
            appendSupplierDealPriceStat(resultFromEs, supplierIdToString);
            // 添加合作采购商统计
            appendCooperatedPurchaserState(resultFromEs, supplierIdToString);
            // 添加发布产品
            appendSupplierProductStat(resultFromEs, supplierIdToString);

            // 保存到es
            batchExecute(resultFromEs);
            scrollResp = elasticClient.getTransportClient().prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute().actionGet();
            pageNumberToUse++;
        } while (scrollResp.getHits().getHits().length != 0);
        logger.info("同步供应商参与的项目统计结束");
    }

    /**
     * 添加已参与项目统计
     *
     * @param resultFromEs
     * @param supplierIds
     */
    private void appendSupplierProjectStat(List<Map<String, Object>> resultFromEs, String supplierIds) {
        // 采购项目
        String queryPurchaseProjectSqlTemplate = "SELECT\n"
                + "   COUNT(1),\n"
                + "   supplier_id\n"
                + "FROM\n"
                + "   purchase_supplier_project where quote_status in (2,3) AND supplier_id in (%s)\n"
                + "GROUP BY\n"
                + "   supplier_id";
        Map<String, Long> purchaseProjectStat = getSupplierStatMap(purchaseDataSource, queryPurchaseProjectSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object value = purchaseProjectStat.get(source.get(ID));
            source.put(TOTAL_PURCHASE_PROJECT, (value == null ? 0 : value));
        }

        // 招标项目
        String queryBidProjectSqlTemplate = "select\n"
                + "    count(1),\n"
                + "    supplier_id AS supplierId\n"
                + " from\n"
                + "    bid_supplier\n"
                + "    where bid_status = 1 AND supplier_id in (%s)\n"
                + " GROUP BY\n"
                + "    supplier_id";
        Map<String, Long> bidProjectStat = getSupplierStatMap(tenderDataSource, queryBidProjectSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object value = bidProjectStat.get(source.get(ID));
            source.put(TOTAL_BID_PROJECT, (value == null ? 0 : value));
        }

        // 已参与总项目个数
        resultFromEs.forEach(map -> {
            Long totalPurchaseProject = Long.valueOf(map.get(TOTAL_PURCHASE_PROJECT).toString());
            Long totalBidProject = Long.valueOf(map.get(TOTAL_BID_PROJECT).toString());
            map.put(TOTAL_PROJECT, totalPurchaseProject + totalBidProject);
        });
    }

    /**
     * 添加已成交项目统计
     *
     * @param resultFromEs
     * @param supplierIds
     */
    private void appendSupplierDealProjectStat(List<Map<String, Object>> resultFromEs, String supplierIds) {
        // 采购项目
        String queryPurchaseProjectSqlTemplate = "SELECT\n"
                + "   COUNT(1),\n"
                + "   supplier_id\n"
                + "FROM\n"
                + "   purchase_supplier_project where deal_status = 2 AND supplier_id in (%s)\n"
                + "GROUP BY\n"
                + "   supplier_id";
        Map<String, Long> purchaseProjectStat = getSupplierStatMap(purchaseDataSource, queryPurchaseProjectSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object value = purchaseProjectStat.get(source.get(ID));
            source.put(TOTAL_DEAL_PURCHASE_PROJECT, (value == null ? 0 : value);
        }

        // 招标项目
        String queryBidProjectSqlTemplate = "select\n"
                + "    count(1),\n"
                + "    supplier_id AS supplierId\n"
                + " from\n"
                + "    bid_supplier\n"
                + "      WHERE\n"
                + "         win_bid_status = 1 AND supplier_id in (%s)\n"
                + " GROUP BY\n"
                + "    supplier_id";
        Map<String, Long> bidProjectStat = getSupplierStatMap(tenderDataSource, queryBidProjectSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object value = bidProjectStat.get(source.get(ID));
            source.put(TOTAL_DEAL_BID_PROJECT, (value == null ? 0 : value);
        }

        // 总成交项目数量
        resultFromEs.forEach(map -> {
            Long totalDealPurchaseProject = Long.valueOf(map.get(TOTAL_DEAL_PURCHASE_PROJECT).toString());
            Long totalDealBidProject = Long.valueOf(map.get(TOTAL_DEAL_BID_PROJECT).toString());
            map.put(TOTAL_DEAL_PROJECT, totalDealPurchaseProject + totalDealBidProject);
        });
    }

    private void appendSupplierDealPriceStat(List<Map<String, Object>> resultFromEs, String supplierIds) {
        // 采购项目
        String queryPurchaseDealPriceSqlTemplate = "SELECT\n" +
                "\tsupplier_id,\n" +
                "\tsum( deal_total_price ) \n" +
                "FROM\n" +
                "\t`purchase_supplier_project` \n" +
                "WHERE\n" +
                "\tdeal_status = 2 \n" +
                "\tAND supplier_id IN (%s) \n" +
                "GROUP BY\n" +
                "\tsupplier_id";
        Map<String, Long> purchaseDealPriceStat = getSupplierPriceStatMap(purchaseDataSource, queryPurchaseDealPriceSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object value = purchaseDealPriceStat.get(source.get(ID));
            source.put(TOTAL_DEAL_PURCHASE_PRICE, value == null ? 0 : value);
        }

        // 招标项目
        String queryBidDealPriceSqlTemplate = "SELECT\n" +
                "\tsupplier_id,\n" +
                "\tsum( win_bid_total_price ) \n" +
                "FROM\n" +
                "\t`bid_supplier` \n" +
                "WHERE\n" +
                "\twin_bid_status = 1 \n" +
                "\tAND supplier_id IN (%s) \n" +
                "GROUP BY\n" +
                "\tsupplier_id";
        Map<String, Long> bidDealPriceStat = getSupplierPriceStatMap(tenderDataSource, queryBidDealPriceSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object value = bidDealPriceStat.get(source.get(ID));
            source.put(TOTAL_DEAL_BID_PRICE, value == null ? 0 : value);
            if (value == null) {
                source.put(TOTAL_DEAL_PRICE, source.get(TOTAL_DEAL_PURCHASE_PRICE));
            } else {
                source.put(TOTAL_DEAL_PRICE, Double.valueOf(value.toString()) + Double.valueOf(source.get(TOTAL_DEAL_PURCHASE_PRICE).toString()));
            }
        }
    }

    /**
     * 添加合作采购商统计
     *
     * @param resultFromEs
     * @param supplierIds
     */
    private void appendCooperatedPurchaserState(List<Map<String, Object>> resultFromEs, String supplierIds) {
        String queryCooperatedPurchaserSqlTemplate = "select\n"
                + "   count(1) AS totalCooperatedPurchaser,\n"
                + "   supplier_id AS supplierId\n"
                + "from\n"
                + "   supplier\n"
                + "   WHERE symbiosis_status in (1,2) AND supplier_id in (%s)\n"
                + "GROUP BY\n"
                + "   supplier_id\n";
        Map<String, Long> supplierStat = getSupplierStatMap(uniregDataSource, queryCooperatedPurchaserSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object value = supplierStat.get(source.get(ID));
            source.put(TOTAL_COOPERATED_PURCHASER, (value == null ? 0 : value));
        }
    }

    /**
     * 添加发布产品统计 FIXME 待修改为新平台的产品信息
     *
     * @param resultFromEs
     * @param supplierIds
     */
    private void appendSupplierProductStat(List<Map<String, Object>> resultFromEs, String supplierIds) {
        String querySupplierProductSqlTemplate = "SELECT\n"
                + "   count(1),\n"
                + "   COMPANYID\n"
                + "FROM\n"
                + "   space_product\n"
                + "WHERE\n"
                + "   STATE = 1 AND  COMPANYID in (%s)\n"
                + "GROUP BY\n"
                + "   COMPANYID";
        Map<String, Long> supplierStat = getSupplierStatMap(enterpriseSpaceDataSource, querySupplierProductSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object value = supplierStat.get(source.get(ID));
            source.put(TOTAL_PRODUCT, (value == null ? 0 : value));
        }
    }

    private Map<String, Long> getSupplierStatMap(DataSource dataSource, String querySqlTemplate, String supplierIds) {
        String querySql = String.format(querySqlTemplate, supplierIds);
        return DBUtil.query(dataSource, querySql, null, new DBUtil.ResultSetCallback<Map<String, Long>>() {
            @Override
            public Map<String, Long> execute(ResultSet resultSet) throws SQLException {
                Map<String, Long> map = new HashMap<>();
                while (resultSet.next()) {
                    map.put(String.valueOf(resultSet.getLong(2)), resultSet.getLong(1));
                }
                return map;
            }
        });
    }

    private Map<String, Long> getSupplierPriceStatMap(DataSource dataSource, String querySqlTemplate, String supplierIds) {
        String querySql = String.format(querySqlTemplate, supplierIds);
        return DBUtil.query(dataSource, querySql, null, new DBUtil.ResultSetCallback<Map<String, Long>>() {
            @Override
            public Map<String, Long> execute(ResultSet resultSet) throws SQLException {
                Map<String, Long> map = new HashMap<>();
                while (resultSet.next()) {
                    long supplierId = resultSet.getLong(1);
                    long totalDealPrice = resultSet.getLong(2);
                    map.put(String.valueOf(supplierId), totalDealPrice);
                }
                return map;
            }
        });
    }


//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
