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
package org.apache.dubbo.rpc;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.extension.SPI;

/**
 * ProxyFactory. (API/SPI, Singleton, ThreadSafe)
 */
@SPI("javassist")
public interface ProxyFactory {

    /**
     * create proxy.
     * 创建 Proxy ，在引用服务调用。
     *
     * @param invoker
     * @return proxy
     */
    @Adaptive({Constants.PROXY_KEY})
    <T> T getProxy(Invoker<T> invoker) throws RpcException;

    /**
     * create proxy.
     *
     * @param invoker
     * @return proxy
     */
    @Adaptive({Constants.PROXY_KEY})
    <T> T getProxy(Invoker<T> invoker, boolean generic) throws RpcException;

    /**
     * create invoker.
     *
     * 创建 Invoker ，在暴露服务时调用。
     *
     * @param <T>
     * @param proxy
     * @param type
     * @param url
     * @return invoker
     */
    @Adaptive({Constants.PROXY_KEY})
    <T> Invoker<T> getInvoker(T proxy, Class<T> type, URL url) throws RpcException;


    public class ProxyFactory$Adaptive implements org.apache.dubbo.rpc.ProxyFactory {
        private static final org.apache.dubbo.common.logger.Logger logger = org.apache.dubbo.common.logger.LoggerFactory.getLogger(ExtensionLoader.class);
        private java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger(0);

        @Override
        public org.apache.dubbo.rpc.Invoker getInvoker(java.lang.Object arg0, java.lang.Class arg1, org.apache.dubbo.common.URL arg2) throws org.apache.dubbo.rpc.RpcException {
            if (arg2 == null) throw new IllegalArgumentException("url == null");
            org.apache.dubbo.common.URL url = arg2;
            String extName = url.getParameter("proxy", "javassist");
            if(extName == null) throw new IllegalStateException("Fail to get extension(org.apache.dubbo.rpc.ProxyFactory) name from url(" + url.toString() + ") use keys([proxy])");
            org.apache.dubbo.rpc.ProxyFactory extension = null;
            try {
                extension = (org.apache.dubbo.rpc.ProxyFactory)ExtensionLoader.getExtensionLoader(org.apache.dubbo.rpc.ProxyFactory.class).getExtension(extName);
            }catch(Exception e){
                if (count.incrementAndGet() == 1) {
                    logger.warn("Failed to find extension named " + extName + " for type org.apache.dubbo.rpc.ProxyFactory, will use default extension javassist instead.", e);
                }
                extension = (org.apache.dubbo.rpc.ProxyFactory)ExtensionLoader.getExtensionLoader(org.apache.dubbo.rpc.ProxyFactory.class).getExtension("javassist");
            }
            return extension.getInvoker(arg0, arg1, arg2);
        }
        @Override
        public java.lang.Object getProxy(org.apache.dubbo.rpc.Invoker arg0, boolean arg1) throws org.apache.dubbo.rpc.RpcException {
            if (arg0 == null) throw new IllegalArgumentException("org.apache.dubbo.rpc.Invoker argument == null");
            if (arg0.getUrl() == null) throw new IllegalArgumentException("org.apache.dubbo.rpc.Invoker argument getUrl() == null");org.apache.dubbo.common.URL url = arg0.getUrl();
            String extName = url.getParameter("proxy", "javassist");
            if(extName == null) throw new IllegalStateException("Fail to get extension(org.apache.dubbo.rpc.ProxyFactory) name from url(" + url.toString() + ") use keys([proxy])");
            org.apache.dubbo.rpc.ProxyFactory extension = null;
            try {
                extension = (org.apache.dubbo.rpc.ProxyFactory)ExtensionLoader.getExtensionLoader(org.apache.dubbo.rpc.ProxyFactory.class).getExtension(extName);
            }catch(Exception e){
                if (count.incrementAndGet() == 1) {
                    logger.warn("Failed to find extension named " + extName + " for type org.apache.dubbo.rpc.ProxyFactory, will use default extension javassist instead.", e);
                }
                extension = (org.apache.dubbo.rpc.ProxyFactory)ExtensionLoader.getExtensionLoader(org.apache.dubbo.rpc.ProxyFactory.class).getExtension("javassist");
            }
            return extension.getProxy(arg0, arg1);
        }
        @Override
        public java.lang.Object getProxy(org.apache.dubbo.rpc.Invoker arg0) throws org.apache.dubbo.rpc.RpcException {
            if (arg0 == null) throw new IllegalArgumentException("org.apache.dubbo.rpc.Invoker argument == null");
            if (arg0.getUrl() == null) throw new IllegalArgumentException("org.apache.dubbo.rpc.Invoker argument getUrl() == null");org.apache.dubbo.common.URL url = arg0.getUrl();
            String extName = url.getParameter("proxy", "javassist");
            if(extName == null) throw new IllegalStateException("Fail to get extension(org.apache.dubbo.rpc.ProxyFactory) name from url(" + url.toString() + ") use keys([proxy])");
            org.apache.dubbo.rpc.ProxyFactory extension = null;
            try {
                extension = (org.apache.dubbo.rpc.ProxyFactory) ExtensionLoader.getExtensionLoader(org.apache.dubbo.rpc.ProxyFactory.class).getExtension(extName);
            }catch(Exception e){
                if (count.incrementAndGet() == 1) {
                    logger.warn("Failed to find extension named " + extName + " for type org.apache.dubbo.rpc.ProxyFactory, will use default extension javassist instead.", e);
                }
                extension = (org.apache.dubbo.rpc.ProxyFactory)ExtensionLoader.getExtensionLoader(org.apache.dubbo.rpc.ProxyFactory.class).getExtension("javassist");
            }
            return extension.getProxy(arg0);
        }
    }

}