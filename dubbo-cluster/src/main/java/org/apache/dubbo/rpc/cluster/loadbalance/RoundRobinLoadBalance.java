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
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.AtomicPositiveInteger;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Round robin load balance.
 */
public class RoundRobinLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "roundrobin";

    private final ConcurrentMap<String, AtomicPositiveInteger> sequences = new ConcurrentHashMap<String, AtomicPositiveInteger>();

    private final ConcurrentMap<String, AtomicPositiveInteger> indexSeqs = new ConcurrentHashMap<String, AtomicPositiveInteger>();

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // key = 全限定类名 + "." + 方法名，比如 com.xxx.DemoService.sayHello
        String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
        // provider的总个数
        int length = invokers.size(); // Number of invokers
        // 最大权重, 只是一个临时值, 所以设置为0, 当遍历invokers时接口的weight肯定大于0，马上就会替换成一个真实的maxWeight的值；
        int maxWeight = 0; // The maximum weight
        // 最小权重，只是一个临时值, 所以设置为Integer类型最大值, 当遍历invokers时接口的weight肯定小于这个数，马上就会替换成一个真实的minWeight的值；
        int minWeight = Integer.MAX_VALUE; // The minimum weight
        final List<Invoker<T>> nonZeroWeightedInvokers = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            // 从Invoker的URL中获取权重时，dubbo会判断是否warnup了，即只有当invoke这个jvm进程的运行时间超过warnup(默认为10分钟)时间，配置的weight才会生效；
            int weight = getWeight(invokers.get(i), invocation);
            // 重新计算最大权重值
            maxWeight = Math.max(maxWeight, weight); // Choose the maximum weight
            // 重新计算最小权重值
            minWeight = Math.min(minWeight, weight); // Choose the minimum weight
            if (weight > 0) {
                nonZeroWeightedInvokers.add(invokers.get(i));
            }
        }
        // 每个方法对应一个AtomicPositiveInteger，其序数从0开始，
        // 查找 key 对应的对应 AtomicPositiveInteger 实例，为空则创建。
        // 这里可以把 AtomicPositiveInteger 看成一个黑盒，大家只要知道
        // AtomicPositiveInteger 用于记录服务的调用编号即可
        AtomicPositiveInteger sequence = sequences.get(key);
        if (sequence == null) {
            sequences.putIfAbsent(key, new AtomicPositiveInteger());
            sequence = sequences.get(key);
        }
        // 如果最小权重小于最大权重，表明服务提供者之间的权重是不相等的
        if (maxWeight > 0 && minWeight < maxWeight) {
            AtomicPositiveInteger indexSeq = indexSeqs.get(key);
            if (indexSeq == null) {
                // 创建 AtomicPositiveInteger，默认值为 -1
                indexSeqs.putIfAbsent(key, new AtomicPositiveInteger(-1));
                indexSeq = indexSeqs.get(key);
            }
            length = nonZeroWeightedInvokers.size();
            while (true) {
                int index = indexSeq.incrementAndGet() % length;
                int currentWeight;
                // 每循环一轮（index = 0），重新计算 currentWeight
                if (index == 0) {
                    currentWeight = sequence.incrementAndGet() % maxWeight;
                } else {
                    currentWeight = sequence.get() % maxWeight;
                }
                // 筛选权重大于当前权重基数的Invoker，从而达到给更大权重的invoke加权的目的
                if (getWeight(nonZeroWeightedInvokers.get(index), invocation) > currentWeight) {
                    return nonZeroWeightedInvokers.get(index);
                }
            }
        }
        // Round robin
        return invokers.get(sequence.getAndIncrement() % length);
    }


    public static void main(String[] args) {
        System.out.println(1%10);
        System.out.println(2%10);
        System.out.println(5%10);
        System.out.println(15%10);
        System.out.println(1%100);
    }
}
