package cn.bidlink.job.ycsearch.handler.projectexpress;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import cn.bidlink.job.ycsearch.handler.JobHandler;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.ValueFilter;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static cn.bidlink.job.common.utils.SyncTimeUtil.getZeroTime;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">wisdom</a>
 * @version Ver 1.0
 * @description:同步名企直通车项目的访问量 cron 一天统计一次 每天23:59
 * @Date 2018/10/29
 */
@Service
@JobHander(value = "syncFamousProjectPageViewDataJobHandler")
public class SyncFamousProjectPageViewDataJobHandler extends JobHandler /*implements InitializingBean*/ {

    private Logger logger = LoggerFactory.getLogger(SyncFamousProjectPageViewDataJobHandler.class);

    @Autowired
    @Qualifier("pageviewDataSource")
    private DataSource pageviewDataSource;

    @Autowired
    private ElasticClient elasticClient;

    private String ID             = "id";
    private String REAL_HIT_COUNT = "realHitCount";
    private String HIT_COUNT      = "hitCount";
    private String URL            = "url";

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("0.开始同步名企直通车的访问量统计");
        syncFamousProjectPageViewData();
        logger.info("1.结束同步名企直通车的访问量统计");
        return ReturnT.SUCCESS;
    }

    private void syncFamousProjectPageViewData() {
        Properties properties = elasticClient.getProperties();
        SearchResponse response = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.express_index"))
                .setTypes(properties.getProperty("cluster.type.project_express_traffic_statistic"))
                .setQuery(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE))
                .setScroll(new TimeValue(60000))
                .setSize(pageSize)
                .execute().actionGet();

        do {
            SearchHits hits = response.getHits();
            List<Map<String, Object>> resultFromEs = new ArrayList<>();
            for (SearchHit hit : hits.getHits()) {
                resultFromEs.add(hit.getSource());
            }
            // 添加点击访问量
            appendHitCount(resultFromEs);

            // 插入数据
            batchInsert(resultFromEs);

            response = elasticClient.getTransportClient().prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute().actionGet();
        } while (response.getHits().getHits().length != 0);
    }

    private void batchInsert(List<Map<String, Object>> mapList) {
        if (!CollectionUtils.isEmpty(mapList)) {
            Properties properties = elasticClient.getProperties();
            BulkRequestBuilder requestBuilder = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> map : mapList) {
                requestBuilder.add(elasticClient.getTransportClient().prepareIndex(properties.getProperty("cluster.express_index"),
                        properties.getProperty("cluster.type.project_express_traffic_statistic"))
                        .setId(String.valueOf(map.get(ID)))
                        .setSource(JSON.toJSONString(map, new ValueFilter() {
                            @Override
                            public Object process(Object object, String propertyKey, Object propertyValue) {
                                if (propertyValue instanceof Date) {
                                    return SyncTimeUtil.toDateString(propertyValue);
                                }
                                return propertyValue;
                            }
                        }))
                );
                BulkResponse response = requestBuilder.execute().actionGet();
                if (response.hasFailures()) {
                    logger.error("名企直通车插入统计访问量数据失败,错误信息:{}", response.buildFailureMessage());
                }
            }
        }
    }

    private void appendHitCount(List<Map<String, Object>> mapList) {
        List<String> urlList = mapList.stream().map(map -> String.valueOf(map.get(URL))).collect(Collectors.toList());
        String querySqlTemplate = "SELECT\n" +
                "\turl,\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\tpage_view_history \n" +
                "WHERE\n" +
                "\turl IN (%s)" +
                "\tAND create_time BETWEEN ? \n" +
                "\tAND ?\n" +
                "GROUP BY\n" +
                "\turl";
        String querySql = String.format(querySqlTemplate, StringUtils.collectionToDelimitedString(urlList, ",", "'", "'"));
        List<Object> params = new ArrayList<>();
        params.add(SyncTimeUtil.toDateString(getZeroTime()));
        params.add(SyncTimeUtil.currentDateToString());
        logger.info("4.查询点击访问量querySql:{}, 参数:{}", querySql, params);
        Map<String, Long> urlPageViewTotalMap = DBUtil.query(pageviewDataSource, querySql, params, new DBUtil.ResultSetCallback<Map<String, Long>>() {
            @Override
            public Map<String, Long> execute(ResultSet resultSet) throws SQLException {
                Map<String, Long> map = new HashMap<>(2);
                while (resultSet.next()) {
                    map.put(resultSet.getString(1), resultSet.getLong(2));
                }
                return map;
            }
        });
        mapList.forEach(map -> {
            Random random = new Random();
            Long pageViewTotal = urlPageViewTotalMap.get(map.get(URL));
            // 真是访问流量
            Object realHitCount = map.get(REAL_HIT_COUNT);
            if (realHitCount == null) {
                // 新名企直通车订单
                map.put(REAL_HIT_COUNT, pageViewTotal == null ? 0 : pageViewTotal);
                map.put(HIT_COUNT, Long.valueOf(map.get(REAL_HIT_COUNT).toString()) + random.nextInt(20) + 30);
            } else {
                // 老订单,加上今天统计的访问量
                if (pageViewTotal != null) {
                    map.put(REAL_HIT_COUNT, Long.valueOf(realHitCount.toString()) + Long.valueOf(pageViewTotal.toString()));
                    map.put(HIT_COUNT, Long.valueOf(map.get(HIT_COUNT).toString()) + Long.valueOf(pageViewTotal.toString()));
                }
            }
        });
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
