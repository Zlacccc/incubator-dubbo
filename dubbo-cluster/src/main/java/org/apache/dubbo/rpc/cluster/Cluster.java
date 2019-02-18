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
package org.apache.dubbo.rpc.cluster;

import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.extension.SPI;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.support.FailoverCluster;

/**
 * Cluster. (SPI, Singleton, ThreadSafe)
 * <p>
 * <a href="http://en.wikipedia.org/wiki/Computer_cluster">Cluster</a>
 * <a href="http://en.wikipedia.org/wiki/Fault-tolerant_system">Fault-Tolerant</a>
 *
 */
@SPI(FailoverCluster.NAME)
public interface Cluster {

    /**
     * Merge the directory invokers to a virtual invoker.
     *
     * @param <T>
     * @param directory
     * @return cluster invoker
     * @throws RpcException
     */
    /**
     * Merge the directory invokers to a virtual invoker.
     *
     * 基于 Directory ，创建 Invoker 对象，实现统一、透明的 Invoker 调用过程
     *
     * @param directory Directory 对象
     * @param <T>  泛型
     * @return cluster invoker
     * @throws RpcException
     */
    @Adaptive
    <T> Invoker<T> join(Directory<T> directory) throws RpcException;

    public class Cluster$Adaptive implements org.apache.dubbo.rpc.cluster.Cluster {
        private static final org.apache.dubbo.common.logger.Logger logger = org.apache.dubbo.common.logger.LoggerFactory.getLogger(ExtensionLoader.class);
        private java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger(0);

        @Override
        public org.apache.dubbo.rpc.Invoker join(org.apache.dubbo.rpc.cluster.Directory arg0) throws org.apache.dubbo.rpc.RpcException {
            if (arg0 == null) throw new IllegalArgumentException("org.apache.dubbo.rpc.cluster.Directory argument == null");
            if (arg0.getUrl() == null) throw new IllegalArgumentException("org.apache.dubbo.rpc.cluster.Directory argument getUrl() == null");org.apache.dubbo.common.URL url = arg0.getUrl();
            String extName = url.getParameter("cluster", "failover");
            if(extName == null) throw new IllegalStateException("Fail to get extension(org.apache.dubbo.rpc.cluster.Cluster) name from url(" + url.toString() + ") use keys([cluster])");
            org.apache.dubbo.rpc.cluster.Cluster extension = null;
            try {
                extension = (org.apache.dubbo.rpc.cluster.Cluster) ExtensionLoader.getExtensionLoader(org.apache.dubbo.rpc.cluster.Cluster.class).getExtension(extName);
            }catch(Exception e){
                if (count.incrementAndGet() == 1) {
                    logger.warn("Failed to find extension named " + extName + " for type org.apache.dubbo.rpc.cluster.Cluster, will use default extension failover instead.", e);
                }
                extension = (org.apache.dubbo.rpc.cluster.Cluster)ExtensionLoader.getExtensionLoader(org.apache.dubbo.rpc.cluster.Cluster.class).getExtension("failover");
            }
            return extension.join(arg0);
        }
    }
}