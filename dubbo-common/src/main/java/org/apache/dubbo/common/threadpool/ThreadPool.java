/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.common.threadpool;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.extension.SPI;

import java.util.concurrent.Executor;

/**
 * ThreadPool
 */
@SPI("fixed")
public interface ThreadPool {

    /**
     * Thread pool
     *
     * @param url URL contains thread parameter
     * @return thread pool
     */
    @Adaptive({Constants.THREADPOOL_KEY})//基于 Dubbo SPI Adaptive 机制，加载对应的线程池实现，使用 URL.threadpool 属性
    Executor getExecutor(URL url);//获得对应的线程池的执行器。


    public class ThreadPool$Adaptive implements org.apache.dubbo.common.threadpool.ThreadPool {
        private static final org.apache.dubbo.common.logger.Logger logger = org.apache.dubbo.common.logger.LoggerFactory.getLogger(ExtensionLoader.class);
        private java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger(0);

        @Override
        public java.util.concurrent.Executor getExecutor(org.apache.dubbo.common.URL arg0) {
            if (arg0 == null) throw new IllegalArgumentException("url == null");
            org.apache.dubbo.common.URL url = arg0;
            String extName = url.getParameter("threadpool", "fixed");
            if (extName == null)
                throw new IllegalStateException("Fail to get extension(org.apache.dubbo.common.threadpool.ThreadPool) name from url(" + url.toString() + ") use keys([threadpool])");
            org.apache.dubbo.common.threadpool.ThreadPool extension = null;
            try {
                extension = (org.apache.dubbo.common.threadpool.ThreadPool) ExtensionLoader.getExtensionLoader(org.apache.dubbo.common.threadpool.ThreadPool.class).getExtension(extName);
            } catch (Exception e) {
                if (count.incrementAndGet() == 1) {
                    logger.warn("Failed to find extension named " + extName + " for type org.apache.dubbo.common.threadpool.ThreadPool, will use default extension fixed instead.", e);
                }
                extension = (org.apache.dubbo.common.threadpool.ThreadPool) ExtensionLoader.getExtensionLoader(org.apache.dubbo.common.threadpool.ThreadPool.class).getExtension("fixed");
            }
            return extension.getExecutor(arg0);
        }
    }
}