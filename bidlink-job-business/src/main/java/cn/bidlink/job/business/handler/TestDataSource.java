package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.utils.DBUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

/**
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/9/22
 */
@JobHander(value = "testDataSource")
@Service
public class TestDataSource extends IJobHandler {
    private Logger logger = LoggerFactory.getLogger(TestDataSource.class);

    @Autowired
    @Qualifier("enterpriseSpaceDataSource")
    private DataSource enterpriseSpaceDataSource;

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        testDataSource();
        return ReturnT.SUCCESS;
    }

    private void testDataSource() {
        logger.info("测试数据源开始");
        testEnterpriseSpaceDataSource();
        logger.info("测试数据源结束");
    }

    private void testEnterpriseSpaceDataSource() {
        logger.info("测试enterpriseSpaceDataSource开始");
        String testYcSql = "SELECT\n"
                           + "   count(1)\n"
                           + " from\n"
                           + "   space_info";
        List<Map<String, Object>> query = DBUtil.query(enterpriseSpaceDataSource, testYcSql, null);
        logger.info("测试enterpriseSpaceDataSource结束");
    }


}
