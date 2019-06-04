package cn.bidlink.job.zznotice;

import cn.bidlink.ctpsp.cloud.service.ZzNoticeService;
import cn.bidlink.ctpsp.dal.entity.ZzNotice;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by Administrator on 2019/6/4.
 */
@Service
public class GetNoticeDetailHandler extends IJobHandler {
    @Autowired
    private ZzNoticeService zzNoticeService;

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        List<ZzNotice> list = zzNoticeService.getUnReadNotice();

        System.out.println(list);

        ReturnT<String> stringReturnT = new ReturnT<>();
        stringReturnT.setMsg("123123haha");
        return stringReturnT;
    }
}
