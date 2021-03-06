package cn.bidlink.job.business.handler;

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
 * 同步商机已报价的供应商数目统计
 *
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2018/1/31
 */
@JobHander("syncBiddenSupplierCountDataJobHandler")
@Service
public class SyncBiddenSupplierCountDataJobHandler extends JobHandler /*implements InitializingBean*/ {
    private Logger logger = LoggerFactory.getLogger(SyncBiddenSupplierCountDataJobHandler.class);

    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    @Qualifier("tenderDataSource")
    protected DataSource tenderDataSource;

    @Autowired
    @Qualifier("purchaseDataSource")
    protected DataSource purchaseDataSource;

    @Autowired
    @Qualifier("auctionDataSource")
    protected DataSource auctionDataSource;

    @Autowired
    @Qualifier("vendueDataSource")
    protected DataSource vendueDataSource;

    @Value("${pageSize}")
    protected int pageSize;

    // 招标项目类型
    protected int BIDDING_PROJECT_TYPE  = 1;
    // 采购项目类型
    protected int PURCHASE_PROJECT_TYPE = 2;
    // 竞价项目
    protected int AUCTION_PROJECT_TYPE  = 3;

    // 拍卖项目
    protected int SALE_PROJECT_TYPE = 5;

    protected String ID                    = "id";
    protected String PROJECT_ID            = "projectId";
    protected String PURCHASE_ID           = "purchaseId";
    protected String BIDDEN_SUPPLIER_COUNT = "biddenSupplierCount";
    protected String PROJECT_TYPE          = "projectType";
    protected String BID_PROJECT_TYPE      = "bidProjectType";

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
//                .must(QueryBuilders.termQuery("status", 1))
                .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.IXIETONG_SOURCE));  // 新平台

        Properties properties = elasticClient.getProperties();
        SearchResponse scrollResp = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.opportunity_index"))
                .setTypes(properties.getProperty("cluster.type.supplier_opportunity"))
                .setQuery(queryBuilder)
                .setScroll(new TimeValue(60000))
                .setFetchSource(new String[]{ID, PROJECT_ID, PURCHASE_ID, PROJECT_TYPE, BID_PROJECT_TYPE}, null)
                .setSize(pageSize)
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
            // 资格预审项目
            List<Map<String, Object>> prequalificationBidProjectSource = new ArrayList<>();
            Set<Pair> prequalificationBidProjectPairs = new HashSet<>();
            // 竞价项目
            List<Map<String, Object>> auctionProjectSource = new ArrayList<>();
            Set<Pair> auctionProjectPairs = new HashSet<>();
            // 拍卖项目
            List<Map<String, Object>> saleProjectSource = new ArrayList<>();
            Set<Pair> saleProjectPairs = new HashSet<>();

            for (SearchHit searchHit : searchHits) {
                Map<String, Object> source = searchHit.getSourceAsMap();
                Integer projectType = (Integer) source.get(PROJECT_TYPE);
                if (projectType != null) {
                    if (projectType == PURCHASE_PROJECT_TYPE) {
                        purchaseProjectSource.add(source);
                        Long projectId = Long.valueOf(String.valueOf(source.get(PROJECT_ID)));
                        Long companyId = Long.valueOf(String.valueOf(source.get(PURCHASE_ID)));
                        purchaseProjectPairs.add(new Pair(companyId, projectId));
                    } else if (projectType == BIDDING_PROJECT_TYPE) {
                        Long projectId = Long.valueOf(String.valueOf(source.get(PROJECT_ID)));
                        Long companyId = Long.valueOf(String.valueOf(source.get(PURCHASE_ID)));
                        Integer bidProjectType = Integer.valueOf(source.get(BID_PROJECT_TYPE).toString());
                        if (Objects.equals(bidProjectType, 21)) {
                            // 资格预审的招标项目
                            prequalificationBidProjectSource.add(source);
                            prequalificationBidProjectPairs.add(new Pair(companyId, projectId));
                        } else {
                            bidProjectSource.add(source);
                            bidProjectPairs.add(new Pair(companyId, projectId));
                        }
                    } else if (projectType == AUCTION_PROJECT_TYPE) {
                        auctionProjectSource.add(source);
                        Long projectId = Long.valueOf(String.valueOf(source.get(PROJECT_ID)));
                        Long companyId = Long.valueOf(String.valueOf(source.get(PURCHASE_ID)));
                        auctionProjectPairs.add(new Pair(companyId, projectId));
                    } else if (projectType == SALE_PROJECT_TYPE) {
                        saleProjectSource.add(source);
                        Long projectId = Long.valueOf(String.valueOf(source.get(PROJECT_ID)));
                        Long companyId = Long.valueOf(String.valueOf(source.get(PURCHASE_ID)));
                        saleProjectPairs.add(new Pair(companyId, projectId));
                    }
                }
            }

            if (purchaseProjectPairs.size() > 0) {
                syncData(purchaseDataSource, purchaseProjectSource, getPurchaseProjectCountSql(purchaseProjectPairs));
            }

            if (bidProjectPairs.size() > 0) {
                syncData(tenderDataSource, bidProjectSource, getBidProjectCountSql(bidProjectPairs));
            }

            if (prequalificationBidProjectPairs.size() > 0) {
                syncData(tenderDataSource, prequalificationBidProjectSource, getPrequalificationBidProjectCountSql(prequalificationBidProjectPairs));
            }

            if (auctionProjectPairs.size() > 0) {
                syncData(auctionDataSource, auctionProjectSource, getAuctionProjectCountSql(auctionProjectPairs));
            }

            if (saleProjectPairs.size() > 0) {
                syncData(vendueDataSource, saleProjectSource, getSaleProjectCountSql(saleProjectPairs));
            }
            scrollResp = elasticClient.getTransportClient().prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute().actionGet();
        } while (scrollResp.getHits().getHits().length != 0);
    }

    private String getPrequalificationBidProjectCountSql(Set<Pair> projectPairs) {
        String querySqlTemplate = "SELECT\n"
                + "   company_id AS purchaseId,\n"
                + "   sub_project_id AS projectId,\n"
                + "   count(supplier_id) AS biddenSupplierCount\n"
                + "FROM\n"
                + "   (SELECT company_id, sub_project_id, supplier_id FROM bid_prequalification_supplier WHERE bid_status = 1 AND(%s)) s\n"
                + "GROUP BY\n"
                + "   company_id,\n"
                + "   sub_project_id";
        int index = 0;
        StringBuilder whereConditionBuilder = new StringBuilder();
        for (Pair projectPair : projectPairs) {
            if (index > 0) {
                whereConditionBuilder.append(" OR ");
            }
            whereConditionBuilder.append("(company_id=").append(projectPair.companyId)
                    .append(" AND sub_project_id=")
                    .append(projectPair.projectId)
                    .append(") ");
            index++;
        }

        return String.format(querySqlTemplate, whereConditionBuilder.toString());
    }

    private String getPurchaseProjectCountSql(Set<Pair> projectPairs) {
        String querySqlTemplate = "SELECT\n"
                + "   company_id AS purchaseId,\n"
                + "   project_id AS projectId,\n"
                + "   count(supplier_id) AS biddenSupplierCount\n"
                + "FROM\n"
                + "   (SELECT company_id, project_id, supplier_id FROM purchase_supplier_project WHERE company_id IS NOT NULL AND quote_status > 1 AND (%s)) s\n"
                + "GROUP BY\n"
                + "   company_id,\n"
                + "   project_id;\n"
                + "\n";
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

    private String getBidProjectCountSql(Set<Pair> projectPairs) {
        String querySqlTemplate = "SELECT\n"
                + "   company_id AS purchaseId,\n"
                + "   sub_project_id AS projectId,\n"
                + "   count(supplier_id) AS biddenSupplierCount\n"
                + "FROM\n"
                + "   (SELECT company_id, sub_project_id, supplier_id FROM bid_supplier WHERE bid_status = 1 AND(%s)) s\n"
                + "GROUP BY\n"
                + "   company_id,\n"
                + "   sub_project_id";
        int index = 0;
        StringBuilder whereConditionBuilder = new StringBuilder();
        for (Pair projectPair : projectPairs) {
            if (index > 0) {
                whereConditionBuilder.append(" OR ");
            }
            whereConditionBuilder.append("(company_id=").append(projectPair.companyId)
                    .append(" AND sub_project_id=")
                    .append(projectPair.projectId)
                    .append(") ");
            index++;
        }

        return String.format(querySqlTemplate, whereConditionBuilder.toString());
    }

    private String getAuctionProjectCountSql(Set<Pair> auctionProjectPair) {
        String querySqlTemplate = "SELECT\n"
                + "   company_id AS purchaseId,\n"
                + "   project_id AS projectId,\n"
                + "   count(supplier_id) AS biddenSupplierCount\n"
                + "FROM\n"
                + "   (SELECT company_id, project_id, supplier_id FROM auction_supplier_project WHERE company_id IS NOT NULL AND (%s)) s\n"
                + "GROUP BY\n"
                + "   company_id,\n"
                + "   project_id;\n"
                + "\n";
        int index = 0;
        StringBuilder whereConditionBuilder = new StringBuilder();
        for (Pair projectPair : auctionProjectPair) {
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

    private String getSaleProjectCountSql(Set<Pair> saleProjectPairs) {
        String querySqlTemplate = "SELECT\n"
                + "   company_id AS purchaseId,\n"
                + "   project_id AS projectId,\n"
                + "   count(supplier_id) AS biddenSupplierCount\n"
                + "FROM\n"
                + "   (SELECT company_id, project_id, supplier_id FROM vendue_supplier_project WHERE company_id IS NOT NULL AND (%s)) s\n"
                + "GROUP BY\n"
                + "   company_id,\n"
                + "   project_id;\n"
                + "\n";
        int index = 0;
        StringBuilder whereConditionBuilder = new StringBuilder();
        for (Pair projectPair : saleProjectPairs) {
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
                        .prepareUpdate(elasticClient.getProperties().getProperty("cluster.opportunity_index"),
                                elasticClient.getProperties().getProperty("cluster.type.supplier_opportunity"),
                                String.valueOf(result.get(ID)))
                        .setDoc(SyncTimeUtil.handlerDate(result)));
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

    /*@Override
    public void afterPropertiesSet() throws Exception {
        execute();
    }*/
}
