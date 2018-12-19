package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:采购商项目数据同步
 * @Date 2018/5/24
 */
@Service
@JobHander(value = "syncPurchaseProjectDataJobHandler")
public class SyncPurchaseProjectDataJobHandler extends AbstractSyncPurchaseDataJobHandler /*implements InitializingBean*/ {

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("开始同步采购商项目数据");
        syncPurchaseProjectData();
        logger.info("结束同步采购商项目数据");
        return ReturnT.SUCCESS;
    }

    /**
     * 同步采购商参与项目和交易额统计
     * <p>
     * 每次统计覆盖之前的数据
     */
    private void syncPurchaseProjectData() {
        logger.info("同步采购商项目和交易额统计开始");
        Properties properties = elasticClient.getProperties();
        int pageSizeToUse = 2 * pageSize;
        SearchResponse scrollResp = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.index"))
                .setTypes(properties.getProperty("cluster.type.purchase"))
                .setScroll(new TimeValue(60000))
                .setSize(pageSizeToUse)
                .get();

        int pageNumberToUse = 0;
        do {
            SearchHits hits = scrollResp.getHits();
            ArrayList<String> purchaserIds = new ArrayList<>();
            List<Map<String, Object>> resultFromEs = new ArrayList<>();
            for (SearchHit hit : hits.getHits()) {
                purchaserIds.add(((String) hit.getSource().get(ID)));
                resultFromEs.add(hit.getSource());
            }

            String purchaserIdToString = StringUtils.collectionToCommaDelimitedString(purchaserIds);
            // 添加采购项目数量
            this.appendPurchaseProjectCount(resultFromEs, purchaserIdToString);
            // 添加采购交易额
            this.appendPurchaseTradingVolume(resultFromEs, purchaserIdToString);

            // 添加竞价项目数量
            this.appendAuctionProjectCount(resultFromEs, purchaserIdToString);
            // 添加竞价交易额
            this.appendAuctionTradingVolume(resultFromEs, purchaserIdToString);

            // 添加招标项目数量
            this.appendBidProjectCount(resultFromEs, purchaserIdToString);
            // 添加招标交易额
            this.appendBidTradingVolume(resultFromEs, purchaserIdToString);

            // 添加合作供应商数量
            this.appendCooperateSupplierCount(resultFromEs, purchaserIdToString);

            // 添加热门采购品
            this.appendDemandManyProjectItem(resultFromEs, purchaserIdToString);

            this.handlerProjectAndTradingVolumeTotal(resultFromEs);

            batchInsert(resultFromEs);
            scrollResp = elasticClient.getTransportClient().prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute()
                    .actionGet();
            pageNumberToUse++;
        } while (scrollResp.getHits().getHits().length != 0);
        logger.info("同步采购商参与项目和交易统计结束");
    }

    private void handlerProjectAndTradingVolumeTotal(List<Map<String, Object>> resultFromEs) {
        if (!CollectionUtils.isEmpty(resultFromEs)) {
            // 计算总的交易额
            for (Map<String, Object> purchaser : resultFromEs) {
                // 项目数量
                Long purchaseProjectCount = Long.valueOf(purchaser.get(PURCHASE_PROJECT_COUNT).toString());
                Long biddingProjectCount = Long.valueOf(purchaser.get(BID_PROJECT_COUNT).toString());
                Long auctionProjectCount = Long.valueOf(purchaser.get(AUCTION_PROJECT_COUNT).toString());
                purchaser.put(PROJECT_COUNT, (purchaseProjectCount + biddingProjectCount + auctionProjectCount));

                // 交易量
                BigDecimal purchaseTradingVolume = (BigDecimal) purchaser.get(PURCHASE_TRADING_VOLUME);
                BigDecimal biddingTradingVolume = (BigDecimal) purchaser.get(BID_TRADING_VOLUME);
                BigDecimal auctionTradingVolume = (BigDecimal) purchaser.get(AUCTION_TRADING_VOLUME_STR);
                BigDecimal tradingVolume = purchaseTradingVolume.add(biddingTradingVolume).add(auctionTradingVolume);
                purchaser.put(LONG_TRADING_VOLUME, tradingVolume.longValue());
                purchaser.put(PURCHASE_TRADING_VOLUME, purchaseTradingVolume.toString());
                purchaser.put(BID_TRADING_VOLUME, purchaseTradingVolume.toString());
                purchaser.put(AUCTION_TRADING_VOLUME_STR, auctionTradingVolume.toString());
                purchaser.put(TRADING_VOLUME, tradingVolume.toString());
            }
        }
    }

    private void appendDemandManyProjectItem(List<Map<String, Object>> resultFromEs, String purchaserIdToString) {
        String queryPurchaseProjectSqlTemplate = "SELECT\n" +
                "\tcompany_id AS companyId,\n" +
                "\tsum( deal_amount ) AS amount,\n" +
                "\tdirectory_id AS directoryId,\n" +
                "\tNAME AS directoryName \n" +
                "FROM\n" +
                "\tpurchase_supplier_project_item_origin \n" +
                "WHERE\n" +
                "\tdeal_status = 3 and company_id in (%s)\n" +
                "GROUP BY\n" +
                "\tcompany_id,\n" +
                "\tdirectory_id \n" +
                "ORDER BY company_id,amount desc";

        String queryBidProjectSqlTemplate = "\n" +
                "SELECT\n" +
                "\tcompany_id AS companyId,\n" +
                "\tsum( deal_number ) AS amount,\n" +
                "\tdirectory_id AS directoryId,\n" +
                "\tNAME AS directoryName \n" +
                "FROM\n" +
                "\tbid_supplier_project_item_origin \n" +
                "WHERE\n" +
                "\tdeal_status = 1 and company_id in (%s)\n" +
                "GROUP BY\n" +
                "\tcompany_id,\n" +
                "\tdirectory_id \n" +
                "ORDER BY company_id,amount desc";

        if (!StringUtils.isEmpty(purchaserIdToString)) {
            Map<Long, List<ProjectItem>> purchaseProjectItemMap = queryProjectItemMap(purchaserIdToString, queryPurchaseProjectSqlTemplate, purchaseDataSource);

            Map<Long, List<ProjectItem>> bidProjectItemMap = queryProjectItemMap(purchaserIdToString, queryBidProjectSqlTemplate, tenderDataSource);

            // 合并招标项目和采购项目采购品集合
            Map<Long, List<ProjectItem>> map = new HashMap<>();
            if (!(Objects.isNull(purchaseProjectItemMap) || purchaseProjectItemMap.isEmpty())) {
                // 采购项目有采购品成交
                for (Map.Entry<Long, List<ProjectItem>> purchaseProjectItem : purchaseProjectItemMap.entrySet()) {
                    Long companyId = purchaseProjectItem.getKey();
                    List<ProjectItem> projectItemList = purchaseProjectItem.getValue();
                    if (!CollectionUtils.isEmpty(bidProjectItemMap.get(companyId))) {
                        // 有采购采购品 无招标采购品
                        projectItemList.addAll(bidProjectItemMap.get(companyId));
                    }
                    map.put(companyId, projectItemList);
                }
            } else if (!(Objects.isNull(bidProjectItemMap) || bidProjectItemMap.isEmpty())) {
                BeanUtils.copyProperties(bidProjectItemMap, map);
            }

            if (!(Objects.isNull(map) || map.isEmpty())) {
                HashMap<Long, String> resultMap = new HashMap<>();
                for (Map.Entry<Long, List<ProjectItem>> projectItemEntry : map.entrySet()) {
                    Long companyId = projectItemEntry.getKey();
                    List<ProjectItem> projectItemList = projectItemEntry.getValue();
                    // 按照成交数量排序 取集合前6个元素
                    List<ProjectItem> collect = projectItemList.stream().distinct().sorted(Comparator.comparingLong(ProjectItem::getAmount).reversed())
                            .limit(6).collect(Collectors.toList());
                    // 集合中采购品名称
                    List<String> directoryNameList = collect.stream().map(projectItem -> {
                                return projectItem.getDirectoryName();
                            }
                    ).collect(Collectors.toList());
                    // 转为string类型,放入map中
                    resultMap.put(companyId, StringUtils.collectionToDelimitedString(directoryNameList, ","));
                }

                for (Map<String, Object> esMap : resultFromEs) {
                    esMap.put("directoryName", resultMap.get(Long.valueOf(esMap.get(ID).toString())));
                }
            }
        }
    }

    private Map<Long, List<ProjectItem>> queryProjectItemMap(String purchaserIdToString, String queryBidProjectSqlTemplate, DataSource dataSource) {
        String queryBidSql = String.format(queryBidProjectSqlTemplate, purchaserIdToString);
        Map<Long, List<ProjectItem>> projectItemMap = DBUtil.query(dataSource, queryBidSql, null, new DBUtil.ResultSetCallback<Map<Long, List<ProjectItem>>>() {
            @Override
            public Map<Long, List<ProjectItem>> execute(ResultSet resultSet) throws SQLException {
                Map<Long, List<ProjectItem>> bidProjectItem = new HashMap<>();
                while (resultSet.next()) {
                    ProjectItem projectItem = new ProjectItem(resultSet.getLong(2), resultSet.getString(4));
                    long companyId = resultSet.getLong(1);
                    if (CollectionUtils.isEmpty(bidProjectItem.get(companyId))) {
                        // 判断map 中key为 companyId 采购商是否存在,不存在往map中put,存在往value中add 最多添加6个采购品
                        ArrayList<ProjectItem> projectItems = new ArrayList<>();
                        projectItems.add(projectItem);
                        bidProjectItem.put(companyId, projectItems);
                    } else if (bidProjectItem.get(companyId).size() < 6) {
                        bidProjectItem.get(companyId).add(projectItem);
                    }
                }
                return bidProjectItem;
            }
        });
        return projectItemMap;
    }

    private void appendAuctionProjectCount(List<Map<String, Object>> purchasers, String purchaserIdToString) {
        if (!StringUtils.isEmpty(purchaserIdToString)) {
            String ldQuerySqlTemplate = "SELECT\n" +
                    "\tap.company_id AS companyId, \n" +
                    "\tcount( 1 ) AS auctionProjectCount\n" +
                    "FROM\n" +
                    "\tauction_project ap \n" +
                    "WHERE\n" +
                    "\tap.project_status = 5\n" +
                    "\tAND ap.company_id IN (%s) \n" +
                    "GROUP BY\n" +
                    "\tap.company_id";
            Map<Long, Long> ldAuctionProjectCountMap = this.getTotal(auctionDataSource, ldQuerySqlTemplate, purchaserIdToString);

            String ycQuerySqlTemplate = "SELECT\n" +
                    "\tap.comp_id AS companyId, \n" +
                    "\tcount( 1 ) AS auctionProjectCount\n" +
                    "FROM\n" +
                    "\tauction_project ap \n" +
                    "WHERE\n" +
                    "\tap.project_stats = 5\n" +
                    "\tAND ap.comp_id IN (%s) \n" +
                    "GROUP BY\n" +
                    "\tap.comp_id";
            Map<Long, Long> ycAuctionProjectCountMap = this.getTotal(ycDataSource, ycQuerySqlTemplate, purchaserIdToString);

            this.builderProjectCount(purchasers, ldAuctionProjectCountMap, ycAuctionProjectCountMap, AUCTION_PROJECT_TYPE);
        }
    }

    private void appendAuctionTradingVolume(List<Map<String, Object>> resultFromEs, String purchaserIdToString) {
        if (!StringUtils.isEmpty(purchaserIdToString)) {
            String ldQuerySqlTemplate = "SELECT\n" +
                    "\tap.company_id AS companyId ,\n" +
                    "\tsum( ape.deal_total_price ) AS auctionTradingVolumeStr\n" +
                    "FROM\n" +
                    "\tauction_project ap\n" +
                    "\tLEFT JOIN auction_project_ext ape ON ap.id = ape.id \n" +
                    "\tAND ap.company_id = ape.company_id \n" +
                    "WHERE\n" +
                    "\tap.project_status = 5 \n" +
                    "\tAND ape.deal_total_price IS NOT NULL \n" +
                    "\tAND ap.company_id IN (%s) \n" +
                    "GROUP BY\n" +
                    "\tap.company_id";
            Map<Long, BigDecimal> ldAuctionProjectVolumeMap = this.findPurchaserTrading(auctionDataSource, ldQuerySqlTemplate, purchaserIdToString);

            String ycQuerySqlTemplate = "SELECT\n" +
                    "\tap.comp_id AS companyId ,\n" +
                    "\tsum( ap.deal_total_price ) AS auctionTradingVolumeStr\n" +
                    "FROM\n" +
                    "\tauction_project ap\n" +
                    "WHERE\n" +
                    "\tap.project_stats = 5 \n" +
                    "\tAND ap.deal_total_price IS NOT NULL \n" +
                    "\tAND ap.comp_id IN (%s) \n" +
                    "GROUP BY\n" +
                    "\tap.comp_id";
            Map<Long, BigDecimal> ycAuctionProjectVolumeMap = this.findPurchaserTrading(ycDataSource, ycQuerySqlTemplate, purchaserIdToString);

            this.builderTradingVolume(resultFromEs, ldAuctionProjectVolumeMap, ycAuctionProjectVolumeMap, AUCTION_PROJECT_TYPE);
        }
    }

    private void builderTradingVolume(List<Map<String, Object>> resultFromEs, Map<Long, BigDecimal> ldProjectTradingVolumeMap, Map<Long, BigDecimal> ycProjectTradingVolumeMap, int projectType) {
        for (Map<String, Object> map : resultFromEs) {
            Long purchaserId = Long.valueOf(map.get(ID).toString());
            BigDecimal ldProjectVolume = ldProjectTradingVolumeMap.get(purchaserId) == null ? BigDecimal.ZERO : ldProjectTradingVolumeMap.get(purchaserId);
            BigDecimal ycProjectVolume = ycProjectTradingVolumeMap.get(purchaserId) == null ? BigDecimal.ZERO : ycProjectTradingVolumeMap.get(purchaserId);
            if (Objects.equals(projectType, PURCHASE_PROJECT_TYPE)) {
                map.put(PURCHASE_TRADING_VOLUME, ldProjectVolume.add(ycProjectVolume));
            } else if (Objects.equals(projectType, BIDDING_PROJECT_TYPE)) {
                map.put(BID_TRADING_VOLUME, ldProjectVolume.add(ycProjectVolume));
            } else if (Objects.equals(projectType, AUCTION_PROJECT_TYPE)) {
                map.put(AUCTION_TRADING_VOLUME_STR, ldProjectVolume.add(ycProjectVolume));
            }
        }
    }

    private void builderProjectCount(List<Map<String, Object>> resultFromEs, Map<Long, Long> ldProjectCountMap, Map<Long, Long> ycProjectCountMap, int projectType) {
        for (Map<String, Object> map : resultFromEs) {
            long companyId = Long.parseLong(((String) map.get(ID)));
            Long ldCount = ldProjectCountMap.get(companyId) == null ? 0L : ldProjectCountMap.get(companyId);
            Long ycCount = ycProjectCountMap.get(companyId) == null ? 0L : ycProjectCountMap.get(companyId);
            if (Objects.equals(projectType, PURCHASE_PROJECT_TYPE)) {
                map.put(PURCHASE_PROJECT_COUNT, ldCount + ycCount);
            } else if (Objects.equals(projectType, BIDDING_PROJECT_TYPE)) {
                map.put(BID_PROJECT_COUNT, ldCount + ycCount);
            } else if (Objects.equals(projectType, AUCTION_PROJECT_TYPE)) {
                map.put(AUCTION_PROJECT_COUNT, ldCount + ycCount);
            } else if (Objects.equals(projectType, COOPERATE_SUPPLIER_TYPE)) {
                map.put(COOPERATE_SUPPLIER_COUNT, ldCount + ycCount);
            }
        }
    }

    private void appendPurchaseTradingVolume(List<Map<String, Object>> purchasers, String purchaserIdToString) {
        if (!StringUtils.isEmpty(purchaserIdToString)) {
            String ldQueryPurchaserSqlTemplate = "SELECT\n" +
                    "\tpp.company_id AS companyId, \n" +
                    "\tsum( ppe.deal_total_price ) AS purchaseTradingVolume\n" +
                    "FROM\n" +
                    "\tpurchase_project pp\n" +
                    "\tLEFT JOIN purchase_project_ext ppe ON pp.id = ppe.id \n" +
                    "\tAND pp.company_id = ppe.company_id \n" +
                    "\t\n" +
                    "WHERE\n" +
                    "\tpp.process_status IN ( 31, 40 )  \n" +
                    "\tAND ppe.deal_total_price IS NOT NULL \n" +
                    "\tAND pp.company_id IN (%s) \n" +
                    "GROUP BY\n" +
                    "\tpp.company_id;";
            Map<Long, BigDecimal> ldPurchaserTradingVolumeMap = this.findPurchaserTrading(purchaseDataSource, ldQueryPurchaserSqlTemplate, purchaserIdToString);

            String ycQueryPurchaserSqlTemplate = "SELECT\n"
                    + "   bp.comp_id AS companyId,\n"
                    + "   sum(bpe.deal_total_price) AS purchaseTradingVolume \n"
                    + "FROM\n"
                    + "   bmpfjz_project bp\n"
                    + "LEFT JOIN bmpfjz_project_ext bpe ON bp.id = bpe.id AND bp.comp_id = bpe.comp_id\n"
                    + "AND bp.project_status IN (8, 9)\n"
                    + "WHERE bpe.deal_total_price is not null\n"
                    + "AND bp.comp_id IN (%s)\n"
                    + "GROUP BY\n"
                    + "   bp.comp_id";
            Map<Long, BigDecimal> ycPurchaserTradingVolumeMap = this.findPurchaserTrading(ycDataSource, ycQueryPurchaserSqlTemplate, purchaserIdToString);
            this.builderTradingVolume(purchasers, ldPurchaserTradingVolumeMap, ycPurchaserTradingVolumeMap, PURCHASE_PROJECT_TYPE);
        }
    }

    private void appendBidTradingVolume(List<Map<String, Object>> purchasers, String purchaserIdToString) {
        if (!StringUtils.isEmpty(purchaserIdToString)) {
            String ldQueryBidSqlTemplate = "SELECT\n" +
                    "\tbsp.company_id AS companyId,\n" +
                    "\tsum( bs.bid_total_price ) AS bidTradingVolume \n" +
                    "FROM\n" +
                    "\tbid_sub_project bsp\n" +
                    "\tLEFT JOIN bid_supplier bs ON bsp.project_id = bs.project_id \n" +
                    "\tAND bsp.id = bs.sub_project_id \n" +
                    "\tAND bsp.company_id = bs.company_id \n" +
                    "WHERE\n" +
                    "\tbsp.project_status IN ( 2, 3 ) \n" +
                    "\tAND bs.win_bid_status = 1 \n" +
                    "\tAND bs.bid_total_price IS NOT NULL\n" +
                    "\tAND bs.company_id in (%s)\n" +
                    "GROUP BY\n" +
                    "\tbsp.company_id;";
            Map<Long, BigDecimal> ldBidProjectTradingVolumeMap = this.findPurchaserTrading(tenderDataSource, ldQueryBidSqlTemplate, purchaserIdToString);

            String ycQueryBidSqlTemplate = "SELECT\n"
                    + "   bp.company_id AS id,\n"
                    + "   sum(bp.total_bid_price) AS bidTradingVolume\n"
                    + "FROM\n"
                    + "   bid_product bp\n"
                    + "WHERE bp.total_bid_price is not null\n"
                    + "AND bp.company_id IN (%s)\n"
                    + "GROUP BY\n"
                    + "     bp.company_id\n";
            Map<Long, BigDecimal> ycBidProjectTradingVolumeMap = this.findPurchaserTrading(ycDataSource, ycQueryBidSqlTemplate, purchaserIdToString);

            this.builderTradingVolume(purchasers, ldBidProjectTradingVolumeMap, ycBidProjectTradingVolumeMap, BIDDING_PROJECT_TYPE);

        }
    }

    private void appendPurchaseProjectCount(List<Map<String, Object>> purchasers, String purchaserIdToString) {
        if (!StringUtils.isEmpty(purchaserIdToString)) {
            // 隆道云采购项目数量
            String ldQueryPurchaserSqlTemplate = "SELECT\n" +
                    "\tpp.company_id AS companyId, \n" +
                    "\tcount( 1 ) AS purchaseProjectCount\n" +
                    "FROM\n" +
                    "\tpurchase_project pp \n" +
                    "WHERE\n" +
                    "\tpp.process_status IN ( 31, 40 )\n" +
                    "\tAND pp.company_id IN (%s) \n" +
                    "GROUP BY\n" +
                    "\tpp.company_id";
            Map<Long, Long> ldPurchaserProjectCountMap = this.getTotal(purchaseDataSource, ldQueryPurchaserSqlTemplate, purchaserIdToString);

            // 悦采采购项目数量
            String ycQueryPurchaserSqlTemplate = "SELECT\n"
                    + "   bp.comp_id AS companyId,\n"
                    + "   count(1) AS purchaseProjectCount\n"
                    + "FROM\n"
                    + "   bmpfjz_project bp\n"
                    + "WHERE bp.project_status IN (8, 9)\n"
                    + "AND bp.comp_id IN (%s) group by bp.comp_id";
            Map<Long, Long> ycPurchaserProjectCountMap = this.getTotal(ycDataSource, ycQueryPurchaserSqlTemplate, purchaserIdToString);

            this.builderProjectCount(purchasers, ldPurchaserProjectCountMap, ycPurchaserProjectCountMap, PURCHASE_PROJECT_TYPE);
        }
    }

    private Map<Long, Long> getTotal(DataSource dataSource, String querySqlTemplate, String purchaserIdsString) {
        String querySql = String.format(querySqlTemplate, purchaserIdsString);
        Map<Long, Long> purchaseProjectCountMap = DBUtil.query(dataSource, querySql, null, new DBUtil.ResultSetCallback<Map<Long, Long>>() {
            @Override
            public Map<Long, Long> execute(ResultSet resultSet) throws SQLException {
                HashMap<Long, Long> projectCountMap = new HashMap<>();
                while (resultSet.next()) {
                    projectCountMap.put(resultSet.getLong(1), resultSet.getLong(2));
                }
                return projectCountMap;
            }
        });
        return purchaseProjectCountMap;
    }

    private Map<Long, BigDecimal> findPurchaserTrading(DataSource dataSource, String querySqlTemplate, String purchaserIdsString) {
        String querySql = String.format(querySqlTemplate, purchaserIdsString);
        Map<Long, BigDecimal> purchaserTradingDataMap = DBUtil.query(dataSource, querySql, null, new DBUtil.ResultSetCallback<Map<Long, BigDecimal>>() {
            @Override
            public Map<Long, BigDecimal> execute(ResultSet resultSet) throws SQLException {
                HashMap<Long, BigDecimal> projectCountMap = new HashMap<>();
                while (resultSet.next()) {
                    projectCountMap.put(resultSet.getLong(1), resultSet.getBigDecimal(2));
                }
                return projectCountMap;
            }
        });
        return purchaserTradingDataMap;
    }

    private void appendBidProjectCount(List<Map<String, Object>> purchasers, String purchaserIdToString) {
        if (!StringUtils.isEmpty(purchaserIdToString)) {
            String ldQueryBidSqlTemplate = "SELECT\n" +
                    "\tcompany_id AS companyId, \n" +
                    "\tcount( 1 ) AS bidProjectCount\n" +
                    "FROM\n" +
                    "\tbid_sub_project \n" +
                    "WHERE\n" +
                    "\tproject_status IN ( 2, 3 ) \n" +
                    "\tAND company_id IN (%s) \n" +
                    "GROUP BY\n" +
                    "\tcompany_id";
            Map<Long, Long> ldBiddingProjectCountMap = this.getTotal(tenderDataSource, ldQueryBidSqlTemplate, purchaserIdToString);

            String ycQueryBidSqlTemplate = "SELECT\n" +
                    "\tCOMPANY_ID AS companyId, \n" +
                    "\tcount( ID ) AS bidProjectCount\n" +
                    "FROM\n" +
                    "\tproj_inter_project \n" +
                    "WHERE\n" +
                    "\tPROJECT_STATUS IN ( 9, 11 ) AND company_id in (%s)\n" +
                    "GROUP BY\n" +
                    "\tCOMPANY_ID;";
            Map<Long, Long> ycBiddingProjectCountMap = this.getTotal(ycDataSource, ycQueryBidSqlTemplate, purchaserIdToString);

            this.builderProjectCount(purchasers, ldBiddingProjectCountMap, ycBiddingProjectCountMap, BIDDING_PROJECT_TYPE);
        }
    }

    private void appendCooperateSupplierCount(List<Map<String, Object>> resultFromEs, String purchaserIdToString) {
        if (!StringUtils.isEmpty(purchaserIdToString)) {
            String ldQuerySqlTemplate = "SELECT\n" +
                    "\tcompany_id as companyId,\n" +
                    "\tcount(1) as cooperateSupplierCount\n" +
                    "FROM\n" +
                    "\t`supplier` \n" +
                    "WHERE\n" +
                    "\tsymbiosis_status = 2 AND company_id in (%s)\n" +
                    "GROUP BY\n" +
                    "\tcompany_id";
            Map<Long, Long> ldCooperateSupplierCountMap = this.getTotal(uniregDataSource, ldQuerySqlTemplate, purchaserIdToString);

            String ycQuerySqlTemplate = "SELECT\n" +
                    "\tcompany_id ,\n" +
                    "\tcount( 1 )\n" +
                    "FROM\n" +
                    "\tbsm_company_supplier \n" +
                    "WHERE\n" +
                    "\tcompany_id IN (%s) \n" +
                    "\tAND supplier_status = 1 \n" +
                    "GROUP BY\n" +
                    "\tcompany_id";
            Map<Long, Long> ycCooperateSupplierCountMap = this.getTotal(ycDataSource, ycQuerySqlTemplate, purchaserIdToString);

            this.builderProjectCount(resultFromEs, ldCooperateSupplierCountMap, ycCooperateSupplierCountMap, COOPERATE_SUPPLIER_TYPE);
        }
    }

    class ProjectItem {
        /**
         * 采购品成交总数量
         */
        private Long   amount;
        /**
         * 采购品名称
         */
        private String directoryName;

        public Long getAmount() {
            return amount;
        }

        public void setAmount(Long amount) {
            this.amount = amount;
        }

        public String getDirectoryName() {
            return directoryName;
        }

        public void setDirectoryName(String directoryName) {
            this.directoryName = directoryName;
        }

        public ProjectItem() {
        }

        public ProjectItem(Long amount, String directoryName) {
            this.amount = amount;
            this.directoryName = directoryName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ProjectItem that = (ProjectItem) o;

            return directoryName != null ? directoryName.equals(that.directoryName) : that.directoryName == null;
        }

        @Override
        public int hashCode() {
            return directoryName != null ? directoryName.hashCode() : 0;
        }
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
