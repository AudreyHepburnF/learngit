import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;

/**
 * Created by Administrator on 2019/6/4.
 */
public class GetNoticeDetailHandler extends IJobHandler {
    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        //new
        ReturnT<String> stringReturnT = new ReturnT<>();
        stringReturnT.setMsg("123123haha");
        return stringReturnT;
    }
}
