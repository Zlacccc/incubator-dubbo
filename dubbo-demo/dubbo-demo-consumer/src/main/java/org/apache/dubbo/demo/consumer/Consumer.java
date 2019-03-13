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
package org.apache.dubbo.demo.consumer;

import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.demo.DemoService;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Consumer {

    /**
     * To get ipv6 address to work, add
     * System.setProperty("java.net.preferIPv6Addresses", "true");
     * before running your application.
     */
    public static void main(String[] args) {
        /*ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[]{"META-INF/spring/dubbo-demo-consumer.xml"});
        context.start();
        DemoService demoService = (DemoService) context.getBean("demoService"); // get remote service proxy
        while (true) {
            try {
                Thread.sleep(1000);
                String hello = demoService.sayHello("world"); // call remote method
                System.out.println(hello); // get result
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        }*/

        ReferenceConfig<DemoService> reference = new ReferenceConfig<>();
        reference.setApplication(new ApplicationConfig("dubbo-demo-api-consumer"));
        reference.setRegistry(new RegistryConfig("zookeeper://127.0.0.1:2181"));
        reference.setInterface(DemoService.class);
        reference.setTimeout(60000);
        DemoService service = reference.get();
        String message = service.sayHello("dubbo");
        System.out.println(message);
    }

    /*public class proxy0 implements ClassGenerator.DC, EchoService, DemoService {
        // 方法数组
        public static Method[] methods;
        private InvocationHandler handler;

        public proxy0(InvocationHandler invocationHandler) {
            this.handler = invocationHandler;
        }

        public proxy0() {
        }

        public String sayHello(String string) {
            // 将参数存储到 Object 数组中
            Object[] arrobject = new Object[]{string};
            // 调用 InvocationHandler 实现类的 invoke 方法得到调用结果
            Object object = this.handler.invoke(this, methods[0], arrobject);
            // 返回调用结果
            return (String)object;
        }

        *//** 回声测试方法 *//*
        public Object $echo(Object object) {
            Object[] arrobject = new Object[]{object};
            Object object2 = this.handler.invoke(this, methods[1], arrobject);
            return object2;
        }
    }*/
}
