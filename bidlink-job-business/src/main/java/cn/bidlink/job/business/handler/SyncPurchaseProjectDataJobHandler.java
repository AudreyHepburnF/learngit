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
            appendPurchaseProjectCount(resultFromEs, purchaserIdToString);
            // 添加采购交易额
            appendPurchaseTradingVolume(resultFromEs, purchaserIdToString);

            // 添加招标项目数量
            appendBidProjectCount(resultFromEs, purchaserIdToString);
            // 添加招标交易额
            appendBidTradingVolume(resultFromEs, purchaserIdToString);

            // 添加竞价项目数量
            appendAuctionProjectCount(resultFromEs, purchaserIdToString);
            // 添加竞价交易额
            appendAuctionTradingVolume(resultFromEs, purchaserIdToString);

            // 添加合同供应商数量
            appendCooperateSupplierCount(resultFromEs, purchaserIdToString);

            // 添加热门采购品
            appendDemandManyProjectItem(resultFromEs, purchaserIdToString);
            batchInsert(resultFromEs);
            scrollResp = elasticClient.getTransportClient().prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute()
                    .actionGet();
            pageNumberToUse++;
        } while (scrollResp.getHits().getHits().length != 0);
        logger.info("同步采购商参与项目和交易统计结束");
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
                "\tdeal_status = 1 \n" +
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

    private void appendAuctionProjectCount(List<Map<String, Object>> resultFromEs, String purchaserIdToString) {
        // FIXME 待竞价项目开发后统计
        for (Map<String, Object> result : resultFromEs) {
            result.put(AUCTION_PROJECT_COUNT, 0);
        }
    }

    private void appendAuctionTradingVolume(List<Map<String, Object>> resultFromEs, String purchaserIdToString) {
        // FIXME 待竞价项目开发后统计
        for (Map<String, Object> result : resultFromEs) {
            result.put(AUCTION_TRADING_VOLUME, 0);
        }
    }

    private void appendPurchaseTradingVolume(List<Map<String, Object>> purchases, String purchaserIdToString) {
        String queryPurchaserSqlTemplate = "SELECT\n" +
                "\tsum( ppe.deal_total_price ) AS purchaseTradingVolume,\n" +
                "\tpp.company_id AS companyId \n" +
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

        if (!StringUtils.isEmpty(purchaserIdToString)) {
            String queryPurchaseSql = String.format(queryPurchaserSqlTemplate, purchaserIdToString);
            // 根据采购商id查询交易额
            List<Map<String, Object>> purchaseTradingVolumeList = DBUtil.query(purchaseDataSource, queryPurchaseSql, null);
            HashMap<String, BigDecimal> purchaseAttributeMap = new HashMap<>();
            // list<Map>转换为map
            if (!CollectionUtils.isEmpty(purchaseTradingVolumeList)) {
                for (Map<String, Object> map : purchaseTradingVolumeList) {
                    purchaseAttributeMap.put(String.valueOf(map.get(COMPANY_ID)), ((BigDecimal) map.get(PURCHASE_TRADING_VOLUME)));
                }
            }
            // 遍历采购商封装交易额
            for (Map<String, Object> purchase : purchases) {
                // 根据采购商id查询交易额
                BigDecimal purchaseTradingVolume = purchaseAttributeMap.get(purchase.get(ID));
                if (purchaseTradingVolume == null) {
                    purchase.put(PURCHASE_TRADING_VOLUME, BigDecimal.ZERO);
                } else {
                    purchase.put(PURCHASE_TRADING_VOLUME, purchaseTradingVolume);
                }
            }
        }
    }

    private void appendBidTradingVolume(List<Map<String, Object>> purchasers, String purchaserIdToString) {
        String queryBidSqlTemplate = "SELECT\n" +
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

        if (!StringUtils.isEmpty(purchaserIdToString)) {
            String queryBidSql = String.format(queryBidSqlTemplate, purchaserIdToString);
            List<Map<String, Object>> bidTradingVolumeList = DBUtil.query(tenderDataSource, queryBidSql, null);
            HashMap<String, BigDecimal> bidAttributeMap = new HashMap<>();
            if (!CollectionUtils.isEmpty(bidTradingVolumeList)) {
                for (Map<String, Object> bidTradingVolumeMap : bidTradingVolumeList) {
                    bidAttributeMap.put(String.valueOf(bidTradingVolumeMap.get(COMPANY_ID)), ((BigDecimal) bidTradingVolumeMap.get(BID_TRADING_VOLUME)));
                }
            }

            // 计算总的交易额
            for (Map<String, Object> purchaser : purchasers) {
                // 采购商招标额
                BigDecimal bidTradingVolume = bidAttributeMap.get(purchaser.get(ID));
                if (bidTradingVolume != null) {
                    //采购商总交易额
                    purchaser.put(BID_TRADING_VOLUME, bidTradingVolume.toString());
                    BigDecimal tradingVolume = ((BigDecimal) purchaser.get(PURCHASE_TRADING_VOLUME)).add(bidTradingVolume);
                    purchaser.put(TRADING_VOLUME, tradingVolume.toString());
                    purchaser.put(LONG_TRADING_VOLUME, tradingVolume.longValue());
                } else {
                    purchaser.put(BID_TRADING_VOLUME, "0");
                    BigDecimal tradingVolume = (BigDecimal) purchaser.get(PURCHASE_TRADING_VOLUME);
                    purchaser.put(TRADING_VOLUME, tradingVolume.toString());
                    purchaser.put(LONG_TRADING_VOLUME, tradingVolume.longValue());
                }
                // 处理为String类型
                purchaser.put(PURCHASE_TRADING_VOLUME, purchaser.get(PURCHASE_TRADING_VOLUME).toString());
            }
        }
    }

    private void appendPurchaseProjectCount(List<Map<String, Object>> purchasers, String purchaserIdToString) {
        String queryPurchaserSqlTemplate = "SELECT\n" +
                "\tcount( 1 ) AS purchaseProjectCount,\n" +
                "\tpp.company_id AS companyId \n" +
                "FROM\n" +
                "\tpurchase_project pp \n" +
                "WHERE\n" +
                "\tpp.process_status IN ( 31, 40 )\n" +
                "\tAND pp.company_id IN (%s) \n" +
                "GROUP BY\n" +
                "\tpp.company_id";
        if (!StringUtils.isEmpty(purchaserIdToString)) {
            String querySql = String.format(queryPurchaserSqlTemplate, purchaserIdToString);
            Map<Long, Long> purchaseProjectCountMap = DBUtil.query(purchaseDataSource, querySql, null, new DBUtil.ResultSetCallback<Map<Long, Long>>() {
                @Override
                public Map<Long, Long> execute(ResultSet resultSet) throws SQLException {
                    HashMap<Long, Long> projectCountMap = new HashMap<>();
                    while (resultSet.next()) {
                        projectCountMap.put(resultSet.getLong("companyId"), resultSet.getLong("purchaseProjectCount"));
                    }
                    return projectCountMap;
                }
            });

            for (Map<String, Object> purchaser : purchasers) {
                Long purchaseProjectCount = purchaseProjectCountMap.get(Long.parseLong(((String) purchaser.get(ID))));
                if (purchaseProjectCount == null) {
                    purchaser.put(PURCHASE_PROJECT_COUNT, 0L);
                } else {
                    purchaser.put(PURCHASE_PROJECT_COUNT, purchaseProjectCount);
                }
            }
        }
    }

    private void appendBidProjectCount(List<Map<String, Object>> purchasers, String purchaserIdToString) {
        String queryBidSqlTemplate = "SELECT\n" +
                "\tcount( 1 ) AS bidProjectCount,\n" +
                "\tcompany_id AS companyId \n" +
                "FROM\n" +
                "\tbid_sub_project \n" +
                "WHERE\n" +
                "\tproject_status IN ( 2, 3 ) \n" +
                "\tAND company_id IN (%s) \n" +
                "GROUP BY\n" +
                "\tcompany_id";

        if (!StringUtils.isEmpty(purchaserIdToString)) {
            String queryBidSql = String.format(queryBidSqlTemplate, purchaserIdToString);
            Map<Long, Long> bidProjectCountMap = DBUtil.query(tenderDataSource, queryBidSql, null, new DBUtil.ResultSetCallback<Map<Long, Long>>() {
                @Override
                public Map<Long, Long> execute(ResultSet resultSet) throws SQLException {
                    HashMap<Long, Long> projectCountMap = new HashMap<>();
                    while (resultSet.next()) {
                        projectCountMap.put(resultSet.getLong(COMPANY_ID), resultSet.getLong(BID_PROJECT_COUNT));
                    }
                    return projectCountMap;
                }
            });
            // 计算总的交易额 FIXME 总项目数量暂时没加竞价项目数量
            for (Map<String, Object> purchaser : purchasers) {
                // 采购商招标项目个数
                Long bidProjectCount = bidProjectCountMap.get(Long.parseLong(((String) purchaser.get(ID))));
                if (bidProjectCount != null) {
                    // 采购商总项目数
                    purchaser.put(BID_PROJECT_COUNT, bidProjectCount);
                    Long projectCount = (Long) purchaser.get(PURCHASE_PROJECT_COUNT) + bidProjectCount;
                    purchaser.put(PROJECT_COUNT, projectCount);
                } else {
                    purchaser.put(BID_PROJECT_COUNT, 0);
                    Long projectCount = (Long) purchaser.get(PURCHASE_PROJECT_COUNT);
                    purchaser.put(PROJECT_COUNT, projectCount);
                }
            }

        }
    }

    private void appendCooperateSupplierCount(List<Map<String, Object>> resultFromEs, String purchaserIdToString) {
        String querySqlTemplate = "SELECT\n" +
                "\tcount(1),\n" +
                "\tcompany_id\n" +
                "FROM\n" +
                "\t`supplier` \n" +
                "WHERE\n" +
                "\tsymbiosis_status = 2 AND company_id in (%s)\n" +
                "GROUP BY\n" +
                "\tcompany_id";
        if (!StringUtils.isEmpty(purchaserIdToString)) {
            String querySql = String.format(querySqlTemplate, purchaserIdToString);
            Map<Long, Long> cooperateSupplierMap = DBUtil.query(uniregDataSource, querySql, null, new DBUtil.ResultSetCallback<Map<Long, Long>>() {
                @Override
                public Map<Long, Long> execute(ResultSet resultSet) throws SQLException {
                    Map<Long, Long> map = new HashMap<>();
                    while (resultSet.next()) {
                        map.put(resultSet.getLong(2), resultSet.getLong(1));
                    }
                    return map;
                }
            });
            for (Map<String, Object> result : resultFromEs) {
                result.put(COOPERATE_SUPPLIER_COUNT, cooperateSupplierMap.get(result.get(ID)) == null ? 0 : cooperateSupplierMap.get(result.get(ID)));
            }
        }
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }

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

}
