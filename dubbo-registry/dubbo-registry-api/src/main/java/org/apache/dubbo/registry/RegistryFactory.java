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
package org.apache.dubbo.registry;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.extension.SPI;

/**
 * RegistryFactory. (SPI, Singleton, ThreadSafe)
 *
 * @see org.apache.dubbo.registry.support.AbstractRegistryFactory
 *
 * 该类是一个支持SPI扩展的类，默认的扩展实现是名称为dubbo的扩展实现类DubboRegistryFactory。
 */
@SPI("dubbo")
public interface RegistryFactory {

    /**
     * Connect to the registry
     * <p>
     * Connecting the registry needs to support the contract: <br>
     * 1. When the check=false is set, the connection is not checked, otherwise the exception is thrown when disconnection <br>
     * 2. Support username:password authority authentication on URL.<br>
     * 3. Support the backup=10.20.153.10 candidate registry cluster address.<br>
     * 4. Support file=registry.cache local disk file cache.<br>
     * 5. Support the timeout=1000 request timeout setting.<br>
     * 6. Support session=60000 session timeout or expiration settings.<br>
     *
     * @param url Registry address, is not allowed to be empty
     * @return Registry reference, never return empty value
     */
    /**
     * 连接注册中心.
     *
     * 连接注册中心需处理契约：<br>
     * 1. 当设置check=false时表示不检查连接，否则在连接不上时抛出异常。<br>
     * 2. 支持URL上的username:password权限认证。<br>
     * 3. 支持backup=10.20.153.10备选注册中心集群地址。<br>
     * 4. 支持file=registry.cache本地磁盘文件缓存。<br>
     * 5. 支持timeout=1000请求超时设置。<br>
     * 6. 支持session=60000会话超时或过期设置。<br>
     *
     * @param url 注册中心地址，不允许为空
     * @return 注册中心引用，总不返回空
     */

    @Adaptive({"protocol"})//@Adaptive({"protocol"}) 注解，Dubbo SPI 会自动实现 RegistryFactory$Adaptive 类，根据 url.protocol 获得对应的 RegistryFactory 实现类。例如，url.protocol = zookeeper 时，获得 ZookeeperRegistryFactory 实现类。
    Registry getRegistry(URL url);


    public class RegistryFactory$Adaptive implements org.apache.dubbo.registry.RegistryFactory {
        private  final org.apache.dubbo.common.logger.Logger logger = org.apache.dubbo.common.logger.LoggerFactory.getLogger(ExtensionLoader.class);
        private java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger(0);
        @Override
        public org.apache.dubbo.registry.Registry getRegistry(org.apache.dubbo.common.URL arg0) {
            if (arg0 == null) throw new IllegalArgumentException("url == null");
            org.apache.dubbo.common.URL url = arg0;
            String extName = ( url.getProtocol() == null ? "dubbo" : url.getProtocol() );
            if(extName == null) throw new IllegalStateException("Fail to get extension(org.apache.dubbo.registry.RegistryFactory) name from url(" + url.toString() + ") use keys([protocol])");
            org.apache.dubbo.registry.RegistryFactory extension = null;
            try {
                extension = (org.apache.dubbo.registry.RegistryFactory) ExtensionLoader.getExtensionLoader(org.apache.dubbo.registry.RegistryFactory.class).getExtension(extName);
            }catch(Exception e){
                if (count.incrementAndGet() == 1) {
                    logger.warn("Failed to find extension named " + extName + " for type org.apache.dubbo.registry.RegistryFactory, will use default extension dubbo instead.", e);
                }
                extension = (org.apache.dubbo.registry.RegistryFactory)ExtensionLoader.getExtensionLoader(org.apache.dubbo.registry.RegistryFactory.class).getExtension("dubbo");
            }
            return extension.getRegistry(arg0);
        }
    }
}