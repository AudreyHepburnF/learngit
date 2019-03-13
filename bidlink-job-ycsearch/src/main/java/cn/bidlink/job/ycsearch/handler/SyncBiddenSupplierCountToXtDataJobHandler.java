package cn.bidlink.job.ycsearch.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">wisdom</a>
 * @version Ver 1.0
 * @description:同步悦采商机项目已参与供应商数量
 * @Date 2018/10/26
 */
@JobHander("syncBiddenSupplierCountToXtDataJobHandler")
@Service
public class SyncBiddenSupplierCountToXtDataJobHandler extends JobHandler implements InitializingBean {
    private Logger logger = LoggerFactory.getLogger(SyncBiddenSupplierCountToXtDataJobHandler.class);

    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    @Qualifier("ycDataSource")
    protected DataSource ycDataSource;

    @Value("${pageSize}")
    protected int pageSize;

    // 招标项目类型
    protected int BIDDING_PROJECT_TYPE  = 1;
    // 采购项目类型
    protected int PURCHASE_PROJECT_TYPE = 2;
    // 竞价项目
    protected int AUCTION_PROJECT_TYPE  = 3;

    protected int RECRUIT_PROJECT_TYPE = 4;

    protected String ID                    = "id";
    protected String PROJECT_ID            = "projectId";
    protected String PURCHASE_ID           = "purchaseId";
    protected String BIDDEN_SUPPLIER_COUNT = "biddenSupplierCount";
    protected String PROJECT_TYPE          = "projectType";

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步商机已报价的供应商数目统计开始");
        syncBiddenSupplierCountData();
        logger.info("同步商机已报价的供应商数目统计结束");
        return ReturnT.SUCCESS;
    }

    /**
     * 同步项目的供应商报价统计
     */
    private void syncBiddenSupplierCountData() {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE))
                .must(QueryBuilders.termQuery("status", 1))
                .mustNot(QueryBuilders.termQuery(PROJECT_TYPE, RECRUIT_PROJECT_TYPE));  // 悦采

        int batchInsert = 100;
        Properties properties = elasticClient.getProperties();
        SearchResponse scrollResp = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.supplier_opportunity_index"))
                .setTypes(properties.getProperty("cluster.type.supplier_opportunity"))
                .setQuery(queryBuilder)
                .setScroll(new TimeValue(60000))
                .setSize(batchInsert)
                .get();
        int i = 0;
        do {
            SearchHit[] searchHits = scrollResp.getHits().getHits();
            // 采购项目
            List<Map<String, Object>> purchaseProjectSource = new ArrayList<>();
            Set<Pair> purchaseProjectPairs = new HashSet<>();
            // 招标项目
            List<Map<String, Object>> bidProjectSource = new ArrayList<>();
            Set<Pair> bidProjectPairs = new HashSet<>();
            // 竞价项目
            List<Map<String, Object>> auctionProjectSource = new ArrayList<>();
            Set<Pair> auctionProjectPairs = new HashSet<>();

            for (SearchHit searchHit : searchHits) {
                Integer projectType = (Integer) searchHit.getSourceAsMap().get(PROJECT_TYPE);
                if (projectType != null) {
                    if (projectType == PURCHASE_PROJECT_TYPE) {
                        purchaseProjectSource.add(searchHit.getSourceAsMap());
                        Long projectId = Long.valueOf(String.valueOf(searchHit.getSourceAsMap().get(PROJECT_ID)));
                        Long companyId = Long.valueOf(String.valueOf(searchHit.getSourceAsMap().get(PURCHASE_ID)));
                        purchaseProjectPairs.add(new Pair(companyId, projectId));
                    } else if (projectType == BIDDING_PROJECT_TYPE) {
                        bidProjectSource.add(searchHit.getSourceAsMap());
                        Long projectId = Long.valueOf(String.valueOf(searchHit.getSourceAsMap().get(PROJECT_ID)));
                        Long companyId = Long.valueOf(String.valueOf(searchHit.getSourceAsMap().get(PURCHASE_ID)));
                        bidProjectPairs.add(new Pair(companyId, projectId));
                    } else if (projectType == AUCTION_PROJECT_TYPE) {
                        auctionProjectSource.add(searchHit.getSourceAsMap());
                        Long projectId = Long.valueOf(String.valueOf(searchHit.getSourceAsMap().get(PROJECT_ID)));
                        Long companyId = Long.valueOf(String.valueOf(searchHit.getSourceAsMap().get(PURCHASE_ID)));
                        auctionProjectPairs.add(new Pair(companyId, projectId));
                    }
                }
            }

            if (purchaseProjectPairs.size() > 0) {
                syncData(ycDataSource, purchaseProjectSource, getPurchaseProjectCountSql(purchaseProjectPairs));
            }

            if (bidProjectPairs.size() > 0) {
                syncData(ycDataSource, bidProjectSource, getBidProjectCountSql(bidProjectPairs));
            }

            if (auctionProjectPairs.size() > 0) {
                syncData(ycDataSource, auctionProjectSource, getAuctionProjectCountSql(auctionProjectPairs));
            }
            scrollResp = elasticClient.getTransportClient().prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute().actionGet();
        } while (scrollResp.getHits().getHits().length != 0);
    }

    private String getPurchaseProjectCountSql(Set<Pair> projectPairs) {
        String querySqlTemplate = "SELECT\n"
                + "   comp_id AS purchaseId,\n"
                + "   project_id AS projectId,\n"
                + "   count(supplier_id) AS biddenSupplierCount\n"
                + "FROM\n"
                + "   (SELECT comp_id, project_id, supplier_id FROM bmpfjz_supplier_project_bid WHERE comp_id IS NOT NULL AND supplier_bid_status IN ( 2, 3, 6, 7 )  AND (%s)) s\n"
                + "GROUP BY\n"
                + "   comp_id,\n"
                + "   project_id;\n"
                + "\n";
        int index = 0;
        StringBuilder whereConditionBuilder = new StringBuilder();
        for (Pair projectPair : projectPairs) {
            if (index > 0) {
                whereConditionBuilder.append(" OR ");
            }
            whereConditionBuilder.append("(comp_id=").append(projectPair.companyId)
                    .append(" AND project_id=")
                    .append(projectPair.projectId)
                    .append(") ");
            index++;
        }

        return String.format(querySqlTemplate, whereConditionBuilder.toString());
    }

    private String getBidProjectCountSql(Set<Pair> projectPairs) {
        String querySqlTemplate = "SELECT\n"
                + "   company_id AS purchaseId,\n"
                + "   project_id AS projectId,\n"
                + "   count(BIDER_ID) AS biddenSupplierCount\n"
                + "FROM\n"
                + "   (SELECT company_id, project_id, BIDER_ID FROM bid WHERE IS_WITHDRAWBID = 0 AND (%s)) s\n"
                + "GROUP BY\n"
                + "   company_id,\n"
                + "   project_id";
        int index = 0;
        StringBuilder whereConditionBuilder = new StringBuilder();
        for (Pair projectPair : projectPairs) {
            if (index > 0) {
                whereConditionBuilder.append(" OR ");
            }
            whereConditionBuilder.append("(company_id=").append(projectPair.companyId)
                    .append(" AND project_id=")
                    .append(projectPair.projectId)
                    .append(") ");
            index++;
        }

        return String.format(querySqlTemplate, whereConditionBuilder.toString());
    }

    private String getAuctionProjectCountSql(Set<Pair> auctionProjectPair) {
        String querySqlTemplate = "SELECT\n"
                + "   comp_id AS purchaseId,\n"
                + "   project_id AS projectId,\n"
                + "   count(supplier_id) AS biddenSupplierCount\n"
                + "FROM\n"
                + "   (SELECT DISTINCT(supplier_id),comp_id, project_id  FROM auction_bid_supplier WHERE comp_id IS NOT NULL AND (%s)) s\n"
                + "GROUP BY\n"
                + "   comp_id,\n"
                + "   project_id;\n"
                + "\n";
        int index = 0;
        StringBuilder whereConditionBuilder = new StringBuilder();
        for (Pair projectPair : auctionProjectPair) {
            if (index > 0) {
                whereConditionBuilder.append(" OR ");
            }
            whereConditionBuilder.append("(comp_id=").append(projectPair.companyId)
                    .append(" AND project_id=")
                    .append(projectPair.projectId)
                    .append(") ");
            index++;
        }

        return String.format(querySqlTemplate, whereConditionBuilder.toString());
    }

    /**
     * 查询对应项目的已报价供应商统计
     *
     * @param sources
     */
    private void syncData(DataSource dataSource, List<Map<String, Object>> sources, String querySql) {
        Map<Pair, Integer> biddenSupplierCountMap = DBUtil.query(dataSource, querySql, null, new DBUtil.ResultSetCallback<Map<Pair, Integer>>() {
            @Override
            public Map<Pair, Integer> execute(ResultSet resultSet) throws SQLException {
                Map<Pair, Integer> map = new HashMap<Pair, Integer>();
                while (resultSet.next()) {
                    long purchaseId = resultSet.getLong(PURCHASE_ID);
                    long projectId = resultSet.getLong(PROJECT_ID);
                    int biddenSupplierCount = resultSet.getInt(BIDDEN_SUPPLIER_COUNT);
                    map.put(new Pair(purchaseId, projectId), biddenSupplierCount);
                }
                return map;
            }
        });

        // 填充供应商统计
        for (Map<String, Object> source : sources) {
            Long purchaseId = Long.valueOf(String.valueOf(source.get(PURCHASE_ID)));
            Long projectId = Long.valueOf(String.valueOf(source.get(PROJECT_ID)));
            Pair key = new Pair(purchaseId, projectId);
            Integer value = biddenSupplierCountMap.get(key);
            source.put(BIDDEN_SUPPLIER_COUNT, (value == null ? 0 : value));
        }

        // 批量插入
        batchExecute(sources);
    }

    protected void batchExecute(List<Map<String, Object>> resultsToUpdate) {
//        System.out.println("size : " + resultsToUpdate.size());
//        for (Map<String, Object> map : resultsToUpdate) {
//            System.out.println(map);
//        }
        if (!CollectionUtils.isEmpty(resultsToUpdate)) {
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> result : resultsToUpdate) {
                bulkRequest.add(elasticClient.getTransportClient()
                        .prepareIndex(elasticClient.getProperties().getProperty("cluster.supplier_opportunity_index"),
                                elasticClient.getProperties().getProperty("cluster.type.supplier_opportunity"),
                                String.valueOf(result.get(ID)))
                        .setSource(SyncTimeUtil.handlerDate(result)));
            }

            BulkResponse response = bulkRequest.execute().actionGet();
            if (response.hasFailures()) {
                logger.error(response.buildFailureMessage());
            }
        }
    }

    class Pair {
        Long companyId;
        Long projectId;

        public Pair(Long companyId, Long projectId) {
            this.companyId = companyId;
            this.projectId = projectId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Pair pair = (Pair) o;

            if (!companyId.equals(pair.companyId)) return false;
            return projectId.equals(pair.projectId);

        }

        @Override
        public int hashCode() {
            int result = companyId.hashCode();
            result = 31 * result + projectId.hashCode();
            return result;
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        execute();
    }
}
