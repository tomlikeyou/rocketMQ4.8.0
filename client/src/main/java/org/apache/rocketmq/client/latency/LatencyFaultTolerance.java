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

package org.apache.rocketmq.client.latency;

public interface LatencyFaultTolerance<T> {
    /**
     * 更新broker条目项
     * @param name broker名称
     * @param currentLatency 消息发送故障的延迟时间（耗时时间）
     * @param notAvailableDuration 该broker不可用的时长（单位：毫秒值），在这个时间范围内，发送消息选择消息队列时会跳过该broker下的所有消息队列，规避掉高延迟、有故障的broker
     */
    void updateFaultItem(final T name, final long currentLatency, final long notAvailableDuration);

    /**
     * 判断指定broker是否可用
     * @param name broker名称
     * @return true：broker可用，false：broker不可用
     */
    boolean isAvailable(final T name);

    /**
     * 移除指定broker，移除掉之后，对于后续的发送消息，代表该broker会重新参与选择消息队列
     * @param name broker名称
     */
    void remove(final T name);

    /**
     * 尝试从故障的broker中重新找到一个可用的broker，没找到返回null
     * @return
     */
    T pickOneAtLeast();
}
