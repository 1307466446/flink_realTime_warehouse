package com.atguigu.gmall.realtime.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Blue红红
 * @description 线程池
 * @create 2022/6/27 19:00
 */
public class ThreadPoolUtil {
    private static ThreadPoolExecutor threadPoolExecutor;

    private ThreadPoolUtil() {

    }

    /**
     * keepLiveTime:没有任务来时，100s就会停掉除核心线程的多余线程
     * @return
     */
    public static ThreadPoolExecutor getThreadPoolExecutor() {
        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolExecutor.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(5, 20, 100, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
                }
            }
        }

        return threadPoolExecutor;
    }
}
