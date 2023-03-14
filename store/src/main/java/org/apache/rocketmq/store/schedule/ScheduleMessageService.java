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
package org.apache.rocketmq.store.schedule;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

public class ScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final long FIRST_DELAY_TIME = 1000L;
    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long DELAY_FOR_A_PERIOD = 10000L;

    /*存储延迟级别 对应的 延迟时间长度（单位：毫秒），key：延迟级别，value：对应的延迟时长（单位：毫秒）*/
    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
        new ConcurrentHashMap<Integer, Long>(32);

    /*存储 延迟级别对应的消费队列的消费进度（该table 每10秒钟，会持久化一次，持久化到本地磁盘）*/
    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
        new ConcurrentHashMap<Integer, Long>(32);

    /*存储主模块*/
    private final DefaultMessageStore defaultMessageStore;

    /*模块启动状态*/
    private final AtomicBoolean started = new AtomicBoolean(false);
    /*定时器 内部有线程资源，可执行调度任务*/
    private Timer timer;
    private MessageStore writeMessageStore;

    /*最大延迟级别*/
    private int maxDelayLevel;

    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.writeMessageStore = defaultMessageStore;
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    /**
     * 计算延迟消费队列ID :延迟级别-1
     * @param delayLevel    延迟级别
     * @return
     */
    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    /**
     * @param writeMessageStore
     *     the writeMessageStore to set
     */
    public void setWriteMessageStore(MessageStore writeMessageStore) {
        this.writeMessageStore = writeMessageStore;
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Map.Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    /**
     * 根据延迟级别计算 消息的到期时间
     * @param delayLevel
     * @param storeTimestamp
     * @return 消息的延迟到期时间
     */
    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            /*创建一个定时器对象（内部有线程资源）*/
            this.timer = new Timer("ScheduleMessageTimerThread", true);

            /*为每个延迟级别，创建一个 “延迟队列任务” 提交到timer，延迟1秒后执行*/
            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
                /*延迟级别*/
                Integer level = entry.getKey();
                /*延迟级别对应的延迟时长*/
                Long timeDelay = entry.getValue();
                /*根据延迟级别 获取对应的消费队列的消费进度*/
                Long offset = this.offsetTable.get(level);
                if (null == offset) {
                    offset = 0L;
                }

                if (timeDelay != null) {
                    this.timer.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME);
                }
            }

            /*提交周期执行的任务，“持久化延迟消费队列的消费进度”，延迟10秒后执行，每10秒钟执行一次*/
            this.timer.scheduleAtFixedRate(new TimerTask() {

                @Override
                public void run() {
                    try {
                        if (started.get()) ScheduleMessageService.this.persist();
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval());
        }
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            if (null != this.timer)
                this.timer.cancel();
        }

    }

    public boolean isStarted() {
        return started.get();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    public String encode() {
        return this.encode(false);
    }

    public boolean load() {
        boolean result = super.load();
        result = result && this.parseDelayLevel();
        return result;
    }

    @Override
    public String configFilePath() {
        /* ../store/config/delayOffset.json*/
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
            .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    public boolean parseDelayLevel() {
        /*该table存储 秒 分 小时  天 对应的 毫秒值*/
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        /*默认支持的延迟级别 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h*/
        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {
            /*按照空格拆分成数组*/
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                /*获取出时间单位：“s” "m" "h"*/
                String ch = value.substring(value.length() - 1);
                /*获取出 时间单位 对应的毫秒值*/
                Long tu = timeUnitTable.get(ch);

                /*延迟级别 从 1级 开始*/
                int level = i + 1;

                if (level > this.maxDelayLevel) {
                    /*记录最大延迟级别*/
                    this.maxDelayLevel = level;
                }
                /*获取出 数值*/
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                /*计算出 延迟级别的 延迟的总毫秒值*/
                long delayTimeMillis = tu * num;
                /*存储到 delayLevelTable 中 方便后面程序使用*/
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    class DeliverDelayedMessageTimerTask extends TimerTask {
        /*延迟队列任务处理的 延迟级别*/
        private final int delayLevel;
        /*延迟队列任务处理的 延迟消费队列的 消费进度*/
        private final long offset;

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        @Override
        public void run() {
            try {
                if (isStarted()) {
                    this.executeOnTimeup();
                }
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                    this.delayLevel, this.offset), DELAY_FOR_A_PERIOD);
            }
        }

        /**
         * @return
         */
        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {

            long result = deliverTimestamp;

            /*计算出一个 now+ 延迟级别对应的延迟毫秒值的 时间戳*/
            long maxTimestamp = now + ScheduleMessageService.this.delayLevelTable.get(this.delayLevel);
            /*条件成立：说明 deliverTimestamp 是有问题的，这里调整为now，让外层立马将该 msg转发到 目标主题*/
            if (deliverTimestamp > maxTimestamp) {
                result = now;
            }

            /*一般情况 result == deliverTimestamp*/
            return result;
        }

        public void executeOnTimeup() {
            /*获取出该延迟队列任务处理的延迟队列 ConsumeQueue 指定延迟主题 队列ID（延迟级别-1）
            * 参数1：延迟主题：SCHEDULE_TOPIC_XXXX
            * 参数2：延迟级别对应的延迟消费队列ID
            * */
            ConsumeQueue cq =
                ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                    delayLevel2QueueId(delayLevel));

            long failScheduleOffset = offset;

            if (cq != null) {
                /*根据 消费进度 查询出 SelectMappedBufferResult 对象*/
                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
                if (bufferCQ != null) {
                    try {
                        long nextOffset = offset;
                        int i = 0;
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        /*依次读取20个字节*/
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            /*延迟消息的物理偏移量*/
                            long offsetPy = bufferCQ.getByteBuffer().getLong();
                            /*延迟消息的消息大小*/
                            int sizePy = bufferCQ.getByteBuffer().getInt();
                            /*延迟消息的 交付时间（ReputMessageService 转发时根据消息的 DELAY 属性是否 > 0，会在tagCode字段 存储交付时间）*/
                            long tagsCode = bufferCQ.getByteBuffer().getLong();

                            if (cq.isExtAddr(tagsCode)) {
                                if (cq.getExt(tagsCode, cqExtUnit)) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    //can't find ext content.So re compute tags code.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}",
                                        tagsCode, offsetPy, sizePy);
                                    long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                                    tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                                }
                            }

                            /*获取系统当前时间*/
                            long now = System.currentTimeMillis();

                            /*延迟消息的交付时间*/
                            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);

                            /*计算下一条 消息的 offset（CQData offset）*/
                            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                            /*差值*/
                            long countdown = deliverTimestamp - now;

                            /*条件成立：说明 msg已经到达交付时间了*/
                            if (countdown <= 0) {
                                /*根据 offsetPy（消息在commitLog的物理偏移量） 和 sizePy（消息size大小） 从 commitLog文件中 读取出 该延迟消息*/
                                MessageExt msgExt =
                                    ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(
                                        offsetPy, sizePy);

                                if (msgExt != null) {
                                    try {
                                        /*根据延迟消息，重新建一条消息，字段大多数copy过滤，修改了主题跟 队列ID*/
                                        MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);
                                        if (TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC.equals(msgInner.getTopic())) {
                                            log.error("[BUG] the real topic of schedule msg is {}, discard the msg. msg={}",
                                                    msgInner.getTopic(), msgInner);
                                            continue;
                                        }
                                        /*将新消息 存储到 commitLog（最终 ReputMessageService 会向 目标主题的 ConsumeQueue 中 添加 CQData）
                                        * 作为消费者 默认都会订阅 重试主题，所以会再次消费该消息
                                        * */
                                        PutMessageResult putMessageResult =
                                            ScheduleMessageService.this.writeMessageStore
                                                .putMessage(msgInner);

                                        if (putMessageResult != null
                                            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                                            continue;
                                        } else {
                                            // XXX: warn and notify me
                                            log.error(
                                                "ScheduleMessageService, a message time up, but reput it failed, topic: {} msgId {}",
                                                msgExt.getTopic(), msgExt.getMsgId());
                                            ScheduleMessageService.this.timer.schedule(
                                                new DeliverDelayedMessageTimerTask(this.delayLevel,
                                                    nextOffset), DELAY_FOR_A_PERIOD);
                                            ScheduleMessageService.this.updateOffset(this.delayLevel,
                                                nextOffset);
                                            return;
                                        }
                                    } catch (Exception e) {
                                        /*
                                         * XXX: warn and notify me



                                         */
                                        log.error(
                                            "ScheduleMessageService, messageTimeup execute error, drop it. msgExt="
                                                + msgExt + ", nextOffset=" + nextOffset + ",offsetPy="
                                                + offsetPy + ",sizePy=" + sizePy, e);
                                    }
                                }
                            } else {
                                /*执行到这里 说明msg 还未到达交付时间*/
                                /*创建 该延迟级别的任务， 延迟 countDown毫秒后 再执行*/
                                ScheduleMessageService.this.timer.schedule(
                                    new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset),
                                    countdown);
                                /*更新 延迟级别对应的消费队列的消费进度*/
                                ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                return;
                            }
                        } // end of for

                        nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                            this.delayLevel, nextOffset), DELAY_FOR_A_WHILE);
                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                        return;
                    } finally {

                        bufferCQ.release();
                    }
                } // end of if (bufferCQ != null)
                else {

                    long cqMinOffset = cq.getMinOffsetInQueue();
                    if (offset < cqMinOffset) {
                        failScheduleOffset = cqMinOffset;
                        log.error("schedule CQ offset invalid. offset=" + offset + ", cqMinOffset="
                            + cqMinOffset + ", queueId=" + cq.getQueueId());
                    }
                }
            } // end of if (cq != null)

            /*重新提交该延迟队列对应的延迟队列任务，延迟100毫秒之后执行*/
            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel,
                failScheduleOffset), DELAY_FOR_A_WHILE);
        }

        private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
            /*新建一条 空消息，新建消息大部分字段都是从 “被延迟消息” 拷贝过来的*/
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());

            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            /*注意 tagsCodeValue不再是 交付时间了*/
            long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            msgInner.setTagsCode(tagsCodeValue);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

            msgInner.setWaitStoreMsgOK(false);
            /*清理新消息的延迟级别属性，为什么要清理呢？ 不清理的话 该消息回头在CommitLog存储时发现延迟级别大于0 又被转发到 延迟主题下（SCHEDULE_TOPIC_XXXX）了*/
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

            /*修改主题为重试主题 %RETRY%消费者组名 */
            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

            /*修改队列ID为 0*/
            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }
    }
}
