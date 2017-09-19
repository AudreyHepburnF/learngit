package cn.bidlink.job.common.utils;

import java.io.IOException;

/**
 * @author : version
 * @version : Ver 1.0
 * @description :
 * @date : 2017/5/5
 */
public class Closer {
    public static void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void closeQuietly(AutoCloseable... closeables) {
        if (closeables != null && closeables.length > 0) {
            for (AutoCloseable closeable : closeables) {
                closeQuietly(closeable);
            }
        }
    }
}
