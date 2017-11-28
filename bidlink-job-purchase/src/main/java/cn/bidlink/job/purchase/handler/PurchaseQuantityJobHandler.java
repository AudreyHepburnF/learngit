package cn.bidlink.job.purchase.handler;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHander;
import org.springframework.stereotype.Service;

@Service
@JobHander
public class PurchaseQuantityJobHandler extends IJobHandler{
    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        // 计算过程
        // 1500 - 2000
        // 基数 2310301230
        return ReturnT.SUCCESS;
    }
}
