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
    @Qualifier("proDataSource")
    private DataSource proDataSource;

    @Autowired
    @Qualifier("ycDataSource")
    private DataSource ycDataSource;

    @Autowired
    @Qualifier("centerDataSource")
    private DataSource centerDataSource;

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        testDataSource();
        return ReturnT.SUCCESS;
    }

    private void testDataSource() {
        logger.info("测试数据源开始");
        testYcDataSource();
        testProDataSource();
        testCenterDataSource();
        logger.info("测试数据源结束");
    }

    private void testYcDataSource() {
        logger.info("测试ycDataSource开始");
        String testYcSql = "SELECT\n"
                           + "   count(1)\n"
                           + " from\n"
                           + "   bmpfjz_supplier_project_bid\n WHERE supplier_bid_status IN (2, 3, 6, 7)";
        List<Map<String, Object>> query = DBUtil.query(ycDataSource, testYcSql, null);
        logger.info("测试ycDataSource结束");
    }

    private void testProDataSource() {
        logger.info("测试proDataSource开始");
        String testYcSql = "SELECT count(1) FROM user_wfirst_use";
        List<Map<String, Object>> query = DBUtil.query(proDataSource, testYcSql, null);
        logger.info("测试proDataSource结束");
    }

    private void testCenterDataSource() {
        logger.info("测试centerDataSource开始");
        String testYcSql = "SELECT count(1) FROM t_reg_company WHERE TYPE = 13 AND MAIN_PRODUCT IS NOT NULL";
        List<Map<String, Object>> query = DBUtil.query(centerDataSource, testYcSql, null);
        logger.info("测试centerDataSource结束");
    }
}
