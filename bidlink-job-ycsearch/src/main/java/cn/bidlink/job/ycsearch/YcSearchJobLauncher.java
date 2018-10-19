package cn.bidlink.job.ycsearch;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.Log4jConfigurer;

import java.io.FileNotFoundException;
import java.util.concurrent.Semaphore;

/**
 * @author : kevin
 * @version : Ver 1.0
 * @description :
 * @date : 2017/5/2
 */
@SuppressWarnings("deprecation")
public class YcSearchJobLauncher {
    public static void main(String[] args) {
        ClassPathXmlApplicationContext context = null;
        try {
            Semaphore semaphore = new Semaphore(0);
            Log4jConfigurer.initLogging("classpath:log4j.xml", 100000);
            context = new ClassPathXmlApplicationContext("classpath:applicationContext.xml");
            final ClassPathXmlApplicationContext finalContext = context;
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    if (finalContext != null) {
                        finalContext.close();
                    }
                }
            }));
            semaphore.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
