package cn.bidlink.job.business.handler;

import com.xxl.job.core.handler.IJobHandler;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Value;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/11/27
 */
public abstract class JobHandler extends IJobHandler {
    @Value("${pageSize:2000}")
    protected int pageSize;

    protected List<Object> appendToParams(List<Object> params, long pageNumber) {
        List<Object> paramsToUse = new ArrayList<>(params);
        paramsToUse.add(pageNumber);
        paramsToUse.add(pageSize);
        return paramsToUse;
    }


}
