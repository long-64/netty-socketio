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
package com.corundumstudio.socketio.ack;

import com.corundumstudio.socketio.*;
import com.corundumstudio.socketio.handler.ClientHead;
import com.corundumstudio.socketio.protocol.Packet;
import com.corundumstudio.socketio.scheduler.CancelableScheduler;
import com.corundumstudio.socketio.scheduler.SchedulerKey;
import com.corundumstudio.socketio.scheduler.SchedulerKey.Type;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class AckManager implements Disconnectable {

    class AckEntry {

        final Map<Long, AckCallback<?>> ackCallbacks = PlatformDependent.newConcurrentHashMap();
        final AtomicLong ackIndex = new AtomicLong(-1);

        public long addAckCallback(AckCallback<?> callback) {
            long index = ackIndex.incrementAndGet();
            ackCallbacks.put(index, callback);
            return index;
        }

        public Set<Long> getAckIndexes() {
            return ackCallbacks.keySet();
        }

        public AckCallback<?> getAckCallback(long index) {
            return ackCallbacks.get(index);
        }

        public AckCallback<?> removeCallback(long index) {
            return ackCallbacks.remove(index);
        }

        public void initAckIndex(long index) {
            ackIndex.compareAndSet(-1, index);
        }

    }

    private static final Logger log = LoggerFactory.getLogger(AckManager.class);

    private final ConcurrentMap<UUID, AckEntry> ackEntries = PlatformDependent.newConcurrentHashMap();

    private final CancelableScheduler scheduler;

    public AckManager(CancelableScheduler scheduler) {
        super();
        this.scheduler = scheduler;
    }

    public void initAckIndex(UUID sessionId, long index) {
        AckEntry ackEntry = getAckEntry(sessionId);
        ackEntry.initAckIndex(index);
    }

    private AckEntry getAckEntry(UUID sessionId) {
        AckEntry ackEntry = ackEntries.get(sessionId);

        // 无则创建 AckEntry,
        if (ackEntry == null) {
            ackEntry = new AckEntry();

            // 存入 Map
            AckEntry oldAckEntry = ackEntries.putIfAbsent(sessionId, ackEntry);
            if (oldAckEntry != null) {
                ackEntry = oldAckEntry;
            }
        }
        return ackEntry;
    }

    /**
     * ack 回调
     * @param client
     * @param packet
     */
    @SuppressWarnings("unchecked")
    public void onAck(SocketIOClient client, Packet packet) {
        // Schedule 任务取消
        AckSchedulerKey key = new AckSchedulerKey(Type.ACK_TIMEOUT, client.getSessionId(), packet.getAckId());
        scheduler.cancel(key);

        // 删除 callback
        AckCallback callback = removeCallback(client.getSessionId(), packet.getAckId());
        if (callback == null) {
            return;
        }

        // 触发 callback -> onSuccess 事件
        if (callback instanceof MultiTypeAckCallback) {
            callback.onSuccess(new MultiTypeArgs(packet.<List<Object>>getData()));
        } else {
            Object param = null;
            List<Object> args = packet.getData();
            if (!args.isEmpty()) {
                param = args.get(0);
            }
            if (args.size() > 1) {
                log.error("Wrong ack args amount. Should be only one argument, but current amount is: {}. Ack id: {}, sessionId: {}",
                        args.size(), packet.getAckId(), client.getSessionId());
            }
            callback.onSuccess(param);
        }
    }

    private AckCallback<?> removeCallback(UUID sessionId, long index) {
        AckEntry ackEntry = ackEntries.get(sessionId);
        // may be null if client disconnected
        // before timeout occurs
        if (ackEntry != null) {
            /*
             * 超时后, 删除 callBack
             */
            return ackEntry.removeCallback(index);
        }
        return null;
    }

    public AckCallback<?> getCallback(UUID sessionId, long index) {
        AckEntry ackEntry = getAckEntry(sessionId);
        return ackEntry.getAckCallback(index);
    }

    public long registerAck(UUID sessionId, AckCallback<?> callback) {

        /**
         *  getAckEntry {@link #getAckEntry(UUID)}
         */
        AckEntry ackEntry = getAckEntry(sessionId);
        ackEntry.initAckIndex(0);
        long index = ackEntry.addAckCallback(callback);

        if (log.isDebugEnabled()) {
            log.debug("AckCallback registered with id: {} for client: {}", index, sessionId);
        }

        /**
         *  AckCallBack {@link #scheduleTimeout(long, UUID, AckCallback)}
         */
        scheduleTimeout(index, sessionId, callback);

        return index;
    }

    private void scheduleTimeout(final long index, final UUID sessionId, AckCallback<?> callback) {
        // -1 无超时时间, 不注册 Schedule
        if (callback.getTimeout() == -1) {
            return;
        }
        SchedulerKey key = new AckSchedulerKey(Type.ACK_TIMEOUT, sessionId, index);

        /**
         *  基于时间轮实现
         *      1、{@link com.corundumstudio.socketio.scheduler.HashedWheelScheduler#scheduleCallback(SchedulerKey, Runnable, long, TimeUnit)}
         *      2、{@link com.corundumstudio.socketio.scheduler.HashedWheelTimeoutScheduler#scheduleCallback(SchedulerKey, Runnable, long, TimeUnit)}
         */
        scheduler.scheduleCallback(key, new Runnable() {
            @Override
            public void run() {
                AckCallback<?> cb = removeCallback(sessionId, index);
                if (cb != null) {
                    /**
                     *  如果在指定时间内,没有回调, 执行超时操作 {@link AckCallback#onTimeout()}
                     */
                    cb.onTimeout();
                }
            }
        }, callback.getTimeout(), TimeUnit.SECONDS);
    }

    /**
     * 断开连接，触发
     * @param client
     */
    @Override
    public void onDisconnect(ClientHead client) {
        // 删除回调
        AckEntry e = ackEntries.remove(client.getSessionId());
        if (e == null) {
            return;
        }

        Set<Long> indexes = e.getAckIndexes();
        for (Long index : indexes) {
            /**
             * 循环执行,超时机制、取消 Schedule
             */
            AckCallback<?> callback = e.getAckCallback(index);
            if (callback != null) {
                callback.onTimeout();
            }
            SchedulerKey key = new AckSchedulerKey(Type.ACK_TIMEOUT, client.getSessionId(), index);
            scheduler.cancel(key);
        }
    }

}
