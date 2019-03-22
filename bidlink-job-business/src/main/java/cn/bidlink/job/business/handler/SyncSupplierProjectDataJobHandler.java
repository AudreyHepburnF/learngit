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
import java.math.BigDecimal;
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
        int pageSizeToUse = 1000;
        SearchResponse scrollResp = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.supplier_index"))
                .setTypes(properties.getProperty("cluster.type.supplier"))
                .setScroll(new TimeValue(60000))
                .setFetchSource(new String[]{ID}, null)
                .setSize(pageSizeToUse)
                .get();

        int pageNumberToUse = 0;
        do {
            SearchHit[] searchHits = scrollResp.getHits().getHits();
            Set<String> supplierIds = new HashSet<>();
            List<Map<String, Object>> resultFromEs = new ArrayList<>();
            for (SearchHit searchHit : searchHits) {
                supplierIds.add(searchHit.getId());
                resultFromEs.add(searchHit.getSourceAsMap());
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
            batchUpdateExecute(resultFromEs);
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
        String ldQueryPurchaseProjectSqlTemplate = "SELECT\n"
                + "   COUNT(1),\n"
                + "   supplier_id\n"
                + "FROM\n"
                + "   purchase_supplier_project where quote_status in (2,3) AND supplier_id in (%s)\n"
                + "GROUP BY\n"
                + "   supplier_id";
        Map<String, Long> ldPurchaseProjectStat = getSupplierStatMap(purchaseDataSource, ldQueryPurchaseProjectSqlTemplate, supplierIds);

        // 采购项目
        String ycQueryPurchaseProjectSqlTemplate = "SELECT\n"
                + "   COUNT(1),\n"
                + "   supplier_id\n"
                + "FROM\n"
                + "   bmpfjz_supplier_project_bid where supplier_bid_status in (2,3,6,7) AND supplier_id in (%s)\n"
                + "GROUP BY\n"
                + "   supplier_id";
        Map<String, Long> ycPurchaseProjectStat = getSupplierStatMap(ycDataSource, ycQueryPurchaseProjectSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object supplierId = source.get(ID);
            long ldPurchaseProjectCount = ldPurchaseProjectStat.get(supplierId) == null ? 0L : ldPurchaseProjectStat.get(supplierId);
            long ycPurchaseProjectCount = ycPurchaseProjectStat.get(supplierId) == null ? 0L : ycPurchaseProjectStat.get(supplierId);
            source.put(TOTAL_PURCHASE_PROJECT, ldPurchaseProjectCount + ycPurchaseProjectCount);
        }


        // 招标项目
        String ldQueryBidProjectSqlTemplate = "select\n"
                + "    count(1),\n"
                + "    supplier_id AS supplierId\n"
                + " from\n"
                + "    bid_supplier\n"
                + "    where bid_status = 1 AND supplier_id in (%s)\n"
                + " GROUP BY\n"
                + "    supplier_id";
        Map<String, Long> ldBidProjectStat = getSupplierStatMap(tenderDataSource, ldQueryBidProjectSqlTemplate, supplierIds);

        // 招标项目
        String ycQueryBidProjectSqlTemplate = "select\n"
                + "    count(1),\n"
                + "    BIDER_ID AS supplierId\n"
                + " from\n"
                + "    bid\n"
                + "    where IS_WITHDRAWBID = 0 AND bider_id in (%s)\n"
                + " GROUP BY\n"
                + "    BIDER_ID";
        Map<String, Long> ycBidProjectStat = getSupplierStatMap(ycDataSource, ycQueryBidProjectSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object supplierId = source.get(ID);
            Long ldBidProjectCount = ldBidProjectStat.get(supplierId) == null ? 0L : ldBidProjectStat.get(supplierId);
            Long ycBidProjectCount = ycBidProjectStat.get(supplierId) == null ? 0L : ycBidProjectStat.get(supplierId);
            source.put(TOTAL_BID_PROJECT, ldBidProjectCount + ycBidProjectCount);
        }

        // 竞价项目 TODO 悦采竞价项目参与量不统计
        String queryAuctionProjectSqlTemplate = "select\n"
                + "    count(1),\n"
                + "    supplier_id AS supplierId\n"
                + " from\n"
                + "    auction_supplier_project\n"
                + "    where quote_status in (2,3) AND supplier_id in (%s)\n"
                + " GROUP BY\n"
                + "    supplier_id";
        Map<String, Long> auctionProjectStat = getSupplierStatMap(auctionDataSource, queryAuctionProjectSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object value = auctionProjectStat.get(source.get(ID));
            source.put(TOTAL_AUCTION_PROJECT, (value == null ? 0 : value));
        }

        // 已参与总项目个数
        resultFromEs.forEach(map -> {
            Long totalPurchaseProject = Long.valueOf(map.get(TOTAL_PURCHASE_PROJECT).toString());
            Long totalBidProject = Long.valueOf(map.get(TOTAL_BID_PROJECT).toString());
            Long totalAuctionProject = Long.valueOf(map.get(TOTAL_AUCTION_PROJECT).toString());
            map.put(TOTAL_PROJECT, totalPurchaseProject + totalBidProject + totalAuctionProject);
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
        String ldQueryPurchaseProjectSqlTemplate = "SELECT\n"
                + "   COUNT(1),\n"
                + "   supplier_id\n"
                + "FROM\n"
                + "   purchase_supplier_project where deal_status = 2 AND supplier_id in (%s)\n"
                + "GROUP BY\n"
                + "   supplier_id";
        Map<String, Long> ldPurchaseProjectStat = getSupplierStatMap(purchaseDataSource, ldQueryPurchaseProjectSqlTemplate, supplierIds);

        // 采购项目
        String ycQueryPurchaseProjectSqlTemplate = "SELECT\n"
                + "   COUNT(1),\n"
                + "   supplier_id\n"
                + "FROM\n"
                + "   bmpfjz_supplier_project_bid where supplier_bid_status = 6 AND supplier_id in (%s)\n"
                + "GROUP BY\n"
                + "   supplier_id";
        Map<String, Long> ycPurchaseProjectStat = getSupplierStatMap(ycDataSource, ycQueryPurchaseProjectSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object supplierId = source.get(ID);
            Long ldPurchaseDealProject = ldPurchaseProjectStat.get(supplierId) == null ? 0L : ldPurchaseProjectStat.get(supplierId);
            Long ycPurchaseDealProject = ycPurchaseProjectStat.get(supplierId) == null ? 0L : ycPurchaseProjectStat.get(supplierId);
            source.put(TOTAL_DEAL_PURCHASE_PROJECT, ldPurchaseDealProject + ycPurchaseDealProject);
        }

        // 招标项目
        String ldQueryBidProjectSqlTemplate = "select\n"
                + "    count(1),\n"
                + "    supplier_id AS supplierId\n"
                + " from\n"
                + "    bid_supplier\n"
                + "      WHERE\n"
                + "         win_bid_status = 1 AND supplier_id in (%s)\n"
                + " GROUP BY\n"
                + "    supplier_id";

        Map<String, Long> ldBidProjectStat = getSupplierStatMap(tenderDataSource, ldQueryBidProjectSqlTemplate, supplierIds);
        // 招标项目
        String ycQueryBidProjectSqlTemplate = "select\n"
                + "    count(1),\n"
                + "    BIDER_ID AS supplierId\n"
                + " from\n"
                + "    bid\n"
                + "      WHERE\n"
                + "         IS_BID_SUCCESS = 1 AND bider_id in (%s)\n"
                + " GROUP BY\n"
                + "    BIDER_ID";

        Map<String, Long> ycBidProjectStat = getSupplierStatMap(ycDataSource, ycQueryBidProjectSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object supplierId = source.get(ID);
            Long ldBidDealProject = ldBidProjectStat.get(supplierId) == null ? 0L : ldBidProjectStat.get(supplierId);
            Long ycBidDealProject = ycBidProjectStat.get(supplierId) == null ? 0L : ycBidProjectStat.get(supplierId);
            source.put(TOTAL_DEAL_BID_PROJECT, ycBidDealProject + ldBidDealProject);
        }

        // 竞价项目
        String ldQueryAuctionProjectSqlTemplate = "select\n"
                + "    count(1),\n"
                + "    supplier_id AS supplierId\n"
                + " from\n"
                + "    auction_supplier_project\n"
                + "      WHERE\n"
                + "         deal_status = 3 AND supplier_id in (%s)\n"
                + " GROUP BY\n"
                + "    supplier_id";
        Map<String, Long> ldAuctionProjectStat = getSupplierStatMap(auctionDataSource, ldQueryAuctionProjectSqlTemplate, supplierIds);

        String ycQueryAuctionProjectSqlTemplate = "SELECT\n" +
                "\tcount( 1 ),\n" +
                "\tsupplier_id \n" +
                "FROM\n" +
                "\tauction_bid_supplier \n" +
                "WHERE\n" +
                "\tbid_status = 1 \n" +
                "\tAND supplier_id IN (%s) \n" +
                "GROUP BY\n" +
                "\tsupplier_id";
        Map<String, Long> ycAuctionProjectStat = getSupplierStatMap(ycDataSource, ycQueryAuctionProjectSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object supplierId = source.get(ID);
            Long ldAuctionDealProject = ldAuctionProjectStat.get(supplierId) == null ? 0L : ldAuctionProjectStat.get(supplierId);
            Long ycAuctionDealProject = ycAuctionProjectStat.get(supplierId) == null ? 0L : ycAuctionProjectStat.get(supplierId);
            source.put(TOTAL_DEAL_AUCTION_PROJECT, ldAuctionDealProject + ycAuctionDealProject);
        }

        // 总成交项目数量
        resultFromEs.forEach(map -> {
            Long totalDealPurchaseProject = Long.valueOf(map.get(TOTAL_DEAL_PURCHASE_PROJECT).toString());
            Long totalDealBidProject = Long.valueOf(map.get(TOTAL_DEAL_BID_PROJECT).toString());
            Long totalDealAuctionProject = Long.valueOf(map.get(TOTAL_DEAL_AUCTION_PROJECT).toString());
            map.put(TOTAL_DEAL_PROJECT, totalDealPurchaseProject + totalDealBidProject + totalDealAuctionProject);
        });
    }

    private void appendSupplierDealPriceStat(List<Map<String, Object>> resultFromEs, String supplierIds) {
        // 采购项目
        String ldQueryPurchaseDealPriceSqlTemplate = "SELECT\n" +
                "\tsupplier_id,\n" +
                "\tsum( deal_total_price ) \n" +
                "FROM\n" +
                "\t`purchase_supplier_project` \n" +
                "WHERE\n" +
                "\tdeal_status = 2 \n" +
                "\tAND supplier_id IN (%s) \n" +
                "GROUP BY\n" +
                "\tsupplier_id";
        Map<String, BigDecimal> ldPurchaseDealPriceStat = getSupplierPriceStatMap(purchaseDataSource, ldQueryPurchaseDealPriceSqlTemplate, supplierIds);

        // 1.采购项目
        String ycQueryPurchaseDealPriceSqlTemplate = "SELECT\n" +
                "\tsupplier_id,\n" +
                "\tsum( deal_total_price ) \n" +
                "FROM\n" +
                "\t`bmpfjz_supplier_project_bid` \n" +
                "WHERE\n" +
                "\tsupplier_bid_status = 6 \n" +
                "\tand supplier_id in (%s)\n" +
                "GROUP BY\n" +
                "\tsupplier_id";
        Map<String, BigDecimal> ycPurchaseDealPriceStat = getSupplierPriceStatMap(ycDataSource, ycQueryPurchaseDealPriceSqlTemplate, supplierIds);

        for (Map<String, Object> source : resultFromEs) {
            Object supplierId = source.get(ID);
            BigDecimal ldTotalDealPurchasePrice = ldPurchaseDealPriceStat.get(supplierId) == null ? BigDecimal.ZERO : ldPurchaseDealPriceStat.get(supplierId);
            BigDecimal ycTotalDealPurchasePrice = ycPurchaseDealPriceStat.get(supplierId) == null ? BigDecimal.ZERO : ycPurchaseDealPriceStat.get(supplierId);
            source.put(TOTAL_DEAL_PURCHASE_PRICE, ldTotalDealPurchasePrice.add(ycTotalDealPurchasePrice));
        }

        // 竞价项目
        String ldQueryAuctionDealPriceSqlTemplate = "SELECT\n" +
                "\tsupplier_id,\n" +
                "\tsum( deal_total_price ) \n" +
                "FROM\n" +
                "\t`auction_supplier_project` \n" +
                "WHERE\n" +
                "\tdeal_status = 3 \n" +
                "\tAND supplier_id IN (%s) \n" +
                "GROUP BY\n" +
                "\tsupplier_id";
        Map<String, BigDecimal> ldAuctionDealPriceStat = getSupplierPriceStatMap(auctionDataSource, ldQueryAuctionDealPriceSqlTemplate, supplierIds);

        // 竞价项目
        String ycQueryAuctionDealPriceSqlTemplate = "SELECT\n" +
                "\tsupplier_id,\n" +
                "\tsum( real_price ) \n" +
                "FROM\n" +
                "\tauction_bid_supplier \n" +
                "WHERE\n" +
                "\tbid_status = 1 \n" +
                "\tAND real_price IS NOT NULL \n" +
                "\tAND supplier_id IN (%s) \n" +
                "GROUP BY\n" +
                "\tsupplier_id";
        Map<String, BigDecimal> ycAuctionDealPriceStat = getSupplierPriceStatMap(ycDataSource, ycQueryAuctionDealPriceSqlTemplate, supplierIds);

        for (Map<String, Object> source : resultFromEs) {
            Object supplierId = source.get(ID);
            BigDecimal ldAuctionDealPrice = ldAuctionDealPriceStat.get(supplierId) == null ? BigDecimal.ZERO : ldAuctionDealPriceStat.get(supplierId);
            BigDecimal ycAuctionDealPrice = ycAuctionDealPriceStat.get(supplierId) == null ? BigDecimal.ZERO : ycAuctionDealPriceStat.get(supplierId);
            source.put(TOTAL_DEAL_AUCTION_PRICE, ldAuctionDealPrice.add(ycAuctionDealPrice));
        }

        // 招标项目
        String ldQueryBidDealPriceSqlTemplate = "SELECT\n" +
                "\tsupplier_id,\n" +
                "\tsum( win_bid_total_price ) \n" +
                "FROM\n" +
                "\t`bid_supplier` \n" +
                "WHERE\n" +
                "\twin_bid_status = 1 \n" +
                "\tAND supplier_id IN (%s) \n" +
                "GROUP BY\n" +
                "\tsupplier_id";
        Map<String, BigDecimal> ldBidDealPriceStat = getSupplierPriceStatMap(tenderDataSource, ldQueryBidDealPriceSqlTemplate, supplierIds);

        // 招标项目
        String ycQueryBidDealPriceSqlTemplate = "SELECT\n" +
                "\tbd.BIDER_ID,\n" +
                "--  bd.BIDER_NAME 供应商名称,\n" +
                "CASE\n" +
                "\t\n" +
                "\tWHEN SUM( bd.BIDER_PRICE_UNE ) > 0 THEN\n" +
                "\tSUM( bd.BIDER_PRICE_UNE ) ELSE 0 \n" +
                "END \n" +
                "FROM\n" +
                "\tproj_inter_project pip\n" +
                "\tINNER JOIN bid bd ON pip.id = bd.PROJECT_ID \n" +
                "\tAND pip.COMPANY_ID = bd.COMPANY_ID \n" +
                "WHERE\n" +
                "\t bd.BIDER_ID IN (%s) \n" +
                "\tAND pip.project_status IN ( 9, 11 ) \n" +
                "GROUP BY\n" +
                "bd.BIDER_ID";
        Map<String, BigDecimal> ycBidDealPriceStat = getSupplierPriceStatMap(ycDataSource, ycQueryBidDealPriceSqlTemplate, supplierIds);

        for (Map<String, Object> source : resultFromEs) {
            Object supplierId = source.get(ID);
            BigDecimal ldBidDealPrice = ldBidDealPriceStat.get(supplierId) == null ? BigDecimal.ZERO : ldBidDealPriceStat.get(supplierId);
            BigDecimal ycBidDealPrice = ycBidDealPriceStat.get(supplierId) == null ? BigDecimal.ZERO : ycBidDealPriceStat.get(supplierId);
            source.put(TOTAL_DEAL_BID_PRICE, ldBidDealPrice.add(ycBidDealPrice));
        }

        resultFromEs.stream().forEach(map -> {
            BigDecimal totalDealPurchasePrice = (BigDecimal) map.get(TOTAL_DEAL_PURCHASE_PRICE);
            BigDecimal totalDealBidPrice = (BigDecimal) map.get(TOTAL_DEAL_BID_PRICE);
            BigDecimal totalDealAuctionPrice = (BigDecimal) map.get(TOTAL_DEAL_AUCTION_PRICE);
            map.put(TOTAL_DEAL_PRICE, totalDealPurchasePrice.add(totalDealBidPrice).add(totalDealAuctionPrice));
        });
    }

    /**
     * 添加合作采购商统计
     *
     * @param resultFromEs
     * @param supplierIds
     */
    private void appendCooperatedPurchaserState(List<Map<String, Object>> resultFromEs, String supplierIds) {
        String ldQueryCooperatedPurchaserSqlTemplate = "select\n"
                + "   count(1) AS totalCooperatedPurchaser,\n"
                + "   supplier_id AS supplierId\n"
                + "from\n"
                + "   supplier\n"
                + "   WHERE symbiosis_status in (1,2) AND supplier_id in (%s)\n"
                + "GROUP BY\n"
                + "   supplier_id\n";
        Map<String, Long> ldSupplierStat = getSupplierStatMap(uniregDataSource, ldQueryCooperatedPurchaserSqlTemplate, supplierIds);

        String ycQueryCooperatedPurchaserSqlTemplate = "SELECT\n" +
                "\tcount( 1 ),\n" +
                "\tsupplier_id \n" +
                "FROM\n" +
                "\tbsm_company_supplier \n" +
                "WHERE\n" +
                "\tsupplier_id IN (%s)\n" +
                "\tAND supplier_status = 1 \n" +
                "GROUP BY\n" +
                "\tsupplier_id";
        Map<String, Long> ycSupplierStat = getSupplierStatMap(ycDataSource, ycQueryCooperatedPurchaserSqlTemplate, supplierIds);
        for (Map<String, Object> source : resultFromEs) {
            Object supplierId = source.get(ID);
            Long ldSupplierCooperatePurchaser = ldSupplierStat.get(supplierId) == null ? 0L : ldSupplierStat.get(supplierId);
            Long ycSupplierCooperatePurchaser = ycSupplierStat.get(supplierId) == null ? 0L : ycSupplierStat.get(supplierId);
            source.put(TOTAL_COOPERATED_PURCHASER, ldSupplierCooperatePurchaser + ycSupplierCooperatePurchaser);
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

    private Map<String, BigDecimal> getSupplierPriceStatMap(DataSource dataSource, String querySqlTemplate, String supplierIds) {
        String querySql = String.format(querySqlTemplate, supplierIds);
        return DBUtil.query(dataSource, querySql, null, new DBUtil.ResultSetCallback<Map<String, BigDecimal>>() {
            @Override
            public Map<String, BigDecimal> execute(ResultSet resultSet) throws SQLException {
                Map<String, BigDecimal> map = new HashMap<>();
                while (resultSet.next()) {
                    long supplierId = resultSet.getLong(1);
                    BigDecimal totalDealPrice = resultSet.getBigDecimal(2);
                    map.put(String.valueOf(supplierId), totalDealPrice);
                }
                return map;
            }
        });
    }


    /*@Override
    public void afterPropertiesSet() throws Exception {
        execute();
    }*/
}
