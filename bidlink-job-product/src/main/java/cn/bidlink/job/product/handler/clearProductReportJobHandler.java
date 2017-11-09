package cn.bidlink.job.product.handler;

import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/9/14
 */
@JobHander(value = "clearProductReportJobHandler")
@Service
public class clearProductReportJobHandler extends IJobHandler /*implements InitializingBean*/ {
    private Logger logger = LoggerFactory.getLogger(clearProductReportJobHandler.class);

    @Autowired
    private ElasticClient elasticClient;

    // 已上架
    private int ON_SALES  = 1;
    // 下架
    private int OFF_SALES = 2;

    private String IS_SALES = "isSales";

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("清理下架的行情产品报告开始");
        clearProductReport();
        logger.info("清理下架的行情产品报告结束");
        return ReturnT.SUCCESS;
    }

    private void clearProductReport() {
        DeleteByQueryResponse response = new DeleteByQueryRequestBuilder(elasticClient.getTransportClient(), DeleteByQueryAction.INSTANCE)
                .setIndices(elasticClient.getProperties().getProperty("cluster.index"))
                .setTypes(elasticClient.getProperties().getProperty("cluster.type.product_report"))
                .setQuery(QueryBuilders.termQuery(IS_SALES, OFF_SALES))
                .execute()
                .actionGet();
        if (response.getTotalFailed() > 0) {
            logger.error("清理下架的行情产品报告失败！");
        }
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
