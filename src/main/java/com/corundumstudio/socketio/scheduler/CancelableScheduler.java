/**
 * Copyright (c) 2012-2019 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.corundumstudio.socketio.scheduler;

import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.TimeUnit;


/**
 *
 *  两个子类，都是基于 Netty 时间轮实现。
 *     {@link HashedWheelTimeoutScheduler}  任务超时后，自动取消
 *     {@link HashedWheelScheduler}  任务超时后, 不自动取消
 *
 */
public interface CancelableScheduler {

    void update(ChannelHandlerContext ctx);

    void cancel(SchedulerKey key);

    void scheduleCallback(SchedulerKey key, Runnable runnable, long delay, TimeUnit unit);

    void schedule(Runnable runnable, long delay, TimeUnit unit);

    void schedule(SchedulerKey key, Runnable runnable, long delay, TimeUnit unit);

    void shutdown();

}
