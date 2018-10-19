//package cn.bidlink.job.business.handler;
//
//import cn.bidlink.job.common.es.ElasticClient;
//import cn.bidlink.job.common.utils.ElasticClientUtil;
//import cn.bidlink.job.common.utils.SyncTimeUtil;
//import com.xxl.job.core.biz.model.ReturnT;
//import com.xxl.job.core.handler.IJobHandler;
//import com.xxl.job.core.handler.annotation.JobHander;
//import org.joda.time.DateTime;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Qualifier;
//import org.springframework.stereotype.Service;
//
//import javax.sql.DataSource;
//import java.sql.Timestamp;
//
///**
// * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
// * @version Ver 1.0
// * @description:同步采购商和供应商采购品归档采购品数据
// * @Date 2018/6/12
// */
//@Service
//@JobHander("syncDealProjectItemDataJobHandler")
//public class SyncDealProjectItemDataJobHandler extends IJobHandler {
//
//    private Logger logger = LoggerFactory.getLogger(SyncDealProjectItemDataJobHandler.class);
//
//    @Autowired
//    private ElasticClient elasticClient;
//
//    @Autowired
//    @Qualifier("purchaseDataSource")
//    private DataSource purchaseDataSource;
//
//    @Autowired
//    @Qualifier("tenderDataSource")
//    private DataSource tenderDataSource;
//
//
//    @Override
//    public ReturnT<String> execute(String... strings) throws Exception {
//        SyncTimeUtil.setCurrentDate();
//        logger.info("开始同步采购商成交项目采购品数据");
//        syncDealProjectItemData();
//        logger.info("结束同步采购商成交项目采购品数据");
//        return ReturnT.SUCCESS;
//    }
//
//    private void syncDealProjectItemData() {
//        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.index", "cluster.type.directory", null);
//        logger.info("同步采购商成交项目采购lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "\n," + "syncTime:" +
//                new DateTime(SyncTimeUtil.getCurrentDate()).toString(SyncTimeUtil.DATE_TIME_PATTERN));
//
//    }
//}
