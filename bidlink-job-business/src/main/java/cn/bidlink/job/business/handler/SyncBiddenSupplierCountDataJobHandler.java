package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.ValueFilter;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
    @Qualifier("ycDataSource")
    protected DataSource ycDataSource;

    // 招标项目类型
    protected int BIDDING_PROJECT_TYPE  = 1;
    // 采购项目类型
    protected int PURCHASE_PROJECT_TYPE = 2;

    protected String ID                    = "id";
    protected String PROJECT_ID            = "projectId";
    protected String PURCHASE_ID           = "purchaseId";
    protected String BIDDEN_SUPPLIER_COUNT = "biddenSupplierCount";

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步商机已报价的供应商数目统计开始");
        syncBiddenSupplierCountData();
        logger.info("同步商机已报价的供应商数目统计结束");
        return ReturnT.SUCCESS;
    }

    private void syncBiddenSupplierCountData() {
        syncPurchaseBiddenSupplierCountData();
        doSyncBidBiddenSupplierCountData();
    }

    /**
     * 同步采购项目的已报价供应商数目统计
     */
    private void syncPurchaseBiddenSupplierCountData() {
        logger.info("同步采购项目的已报价供应商数目统计开始");
        doSyncBiddenSupplierCountData(PURCHASE_PROJECT_TYPE, new QuerySqlBuilder() {
            @Override
            public String build(Set<Pair> projectPairs) {
                String querySqlTemplate = "SELECT\n"
                                          + "   comp_id AS purchaseId,\n"
                                          + "   project_id AS projectId,\n"
                                          + "   count(supplier_id) AS biddenSupplierCount\n"
                                          + "FROM\n"
                                          + "   (SELECT comp_id, project_id, supplier_id FROM bmpfjz_supplier_project_bid WHERE %s) s\n"
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
        });

        logger.info("同步采购项目的已报价供应商数目统计结束");
    }

    /**
     * 同步招标项目的已报价供应商数目统计
     */
    private void doSyncBidBiddenSupplierCountData() {
        logger.info("同步招标项目的已报价供应商数目统计开始");
        doSyncBiddenSupplierCountData(BIDDING_PROJECT_TYPE, new QuerySqlBuilder() {
            @Override
            public String build(Set<Pair> projectPairs) {
                String querySqlTemplate = "SELECT\n"
                                          + "   company_id AS purchaseId,\n"
                                          + "   project_id AS projectId,\n"
                                          + "   count(bider_id) AS biddenSupplierCount\n"
                                          + "FROM\n"
                                          + "   (SELECT company_id, project_id, bider_id FROM bid WHERE %s) s\n"
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
        });

        logger.info("同步招标项目的已报价供应商数目统计结束");
    }

    /**
     * 同步项目的供应商报价统计
     *
     * @param projectType
     * @param querySqlBuilder
     */
    private void doSyncBiddenSupplierCountData(int projectType, QuerySqlBuilder querySqlBuilder) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("status", 1))
                .must(QueryBuilders.termQuery("projectType", projectType));

        int batchSize = 500;
        Properties properties = elasticClient.getProperties();
        SearchResponse scrollResp = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.index"))
                .setTypes(properties.getProperty("cluster.type.supplier_opportunity"))
                .setQuery(queryBuilder)
                .setScroll(new TimeValue(60000))
                .setSize(batchSize)
                .get();
        int i = 0;
        do {
            SearchHit[] searchHits = scrollResp.getHits().hits();
            List<Map<String, Object>> sources = new ArrayList<>();
            Set<Pair> projectPairs = new HashSet<>();
            for (SearchHit searchHit : searchHits) {
                sources.add(searchHit.getSource());
                Long projectId = Long.valueOf(String.valueOf(searchHit.getSource().get(PROJECT_ID)));
                Long companyId = Long.valueOf(String.valueOf(searchHit.getSource().get(PURCHASE_ID)));
                projectPairs.add(new Pair(companyId, projectId));
            }

            if (projectPairs.size() > 0) {
                doSyncBiddenSupplierCountData(sources, projectPairs, querySqlBuilder);
            }
            scrollResp = elasticClient.getTransportClient().prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute().actionGet();
        } while (scrollResp.getHits().getHits().length != 0);
    }

    /**
     * 查询对应项目的已报价供应商统计
     *
     * @param sources
     * @param projectPairs
     * @param querySqlBuilder
     */
    private void doSyncBiddenSupplierCountData(List<Map<String, Object>> sources, Set<Pair> projectPairs, QuerySqlBuilder querySqlBuilder) {
        String querySql = querySqlBuilder.build(projectPairs);
        Map<Pair, Integer> biddenSupplierCountMap = DBUtil.query(ycDataSource, querySql, null, new DBUtil.ResultSetCallback<Map<Pair, Integer>>() {
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
                                        .prepareIndex(elasticClient.getProperties().getProperty("cluster.index"),
                                                      elasticClient.getProperties().getProperty("cluster.type.supplier_opportunity"),
                                                      String.valueOf(result.get(ID)))
                                        .setSource(JSON.toJSONString(result, new ValueFilter() {
                                            @Override
                                            public Object process(Object object, String propertyName, Object propertyValue) {
                                                if (propertyValue instanceof java.util.Date) {
                                                    return new DateTime(propertyValue).toString(SyncTimeUtil.DATE_TIME_PATTERN);
                                                } else {
                                                    return propertyValue;
                                                }
                                            }
                                        })));
            }

            BulkResponse response = bulkRequest.execute().actionGet();
            if (response.hasFailures()) {
                logger.error(response.buildFailureMessage());
            }
        }
    }

    interface QuerySqlBuilder {
        String build(Set<Pair> projectPairs);
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
}
