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
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.SelectMappedBufferResult;

public class HAConnection {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /*上层对象*/
    private final HAService haService;
    /*slave与master之间会话通信的 socketChannel*/
    private final SocketChannel socketChannel;
    /*slave客户端地址*/
    private final String clientAddr;
    /*master节点给slave节点同步数据服务/写数据服务*/
    private WriteSocketService writeSocketService;
    /*读数据服务*/
    private ReadSocketService readSocketService;

    /*默认值：-1，它是在收到slave上报过本地的同步进度之后，被赋值的。它>=0了之后，同步数据的逻辑才会运行，
    * 为什么？因为 master它不知道 slave节点 当前消息存储进度在哪，他就没办法给slave推送数据*/
    private volatile long slaveRequestOffset = -1;
    /*保存slave上报的最新 同步进度信息，slaveAckOffset 之前的数据，都可以认为 slave已经全部同步完成了
    * 对应的 生产者线程需要被唤醒了
    * */
    private volatile long slaveAckOffset = -1;

    public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
        this.haService = haService;
        /*客户端socket通道*/
        this.socketChannel = socketChannel;
        this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
        /*设置非阻塞*/
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);
        /*设置读写缓冲区大小为 64kb*/
        this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
        this.socketChannel.socket().setSendBufferSize(1024 * 64);
        /*创建读写服务*/
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        this.readSocketService = new ReadSocketService(this.socketChannel);
        this.haService.getConnectionCount().incrementAndGet();
    }

    public void start() {
        /*启动读写服务*/
        this.readSocketService.start();
        this.writeSocketService.start();
    }

    public void shutdown() {
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    class ReadSocketService extends ServiceThread {
        /*1MB*/
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        /*多路复用器*/
        private final Selector selector;
        /*master与slave之间的会话socketChannel*/
        private final SocketChannel socketChannel;


        /*slave向master 传输的帧格式：
        * [long][long][long]...
        * slave向master上报的是  slave节点本地的同步进度，这个同步进度是一个long值
        * */
        /*读写缓冲区 1MB*/
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        /*缓冲区处理位点*/
        private int processPosition = 0;
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            /*保存客户端的socket通道*/
            this.socketChannel = socketChannel;
            /*socketChannel注册到多路复用器，关注 OP_READ事件*/
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    /*多路复用器 最长阻塞1秒钟*/
                    this.selector.select(1000);
                    /*两种情况执行到这里：1.事件就绪（OP_READ）2.超时*/
                    /*处理slave节点的拉取请求*/
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }

                    long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        /*长时间未发生 通信的话，结束HAConnection连接*/
                        log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            /*设置serviceThread状态为stopped*/
            this.makeStop();

            /*将读服务对应的写服务 也设置线程状态为 stopped*/
            writeSocketService.makeStop();
            /*移除该HAConnection*/
            haService.removeConnection(HAConnection.this);
            /*减1*/
            HAConnection.this.haService.getConnectionCount().decrementAndGet();

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                /*关闭socket通道*/
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }

        /**
         * 处理读事件
         * 返回boolean：返回true：正常 false：socket处于半关闭状态，需要上层对象重建HAClient对象
         * @return
         */
        private boolean processReadEvent() {
            /*循环控制变量，当连续从socket读取失败 3次（未加载到数据），退出循环*/
            int readSizeZeroTimes = 0;

            /*条件成立：说明  byteBufferRead写满数据了，没有空间了 */
            if (!this.byteBufferRead.hasRemaining()) {
                /*相当于清理操作，pos=0*/
                this.byteBufferRead.flip();
                /*处理位点为0*/
                this.processPosition = 0;
            }

            while (this.byteBufferRead.hasRemaining()) {
                try {
                    /*到socket读缓冲区加载数据，readSize表示加载的数据量*/
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        /*case1：加载成功*/

                        readSizeZeroTimes = 0;

                        /*更新上次读时间*/
                        this.lastReadTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();

                        /*条件成立：说明 byteBufferRead 里 至少包含一个帧数据，说明收到slave节点发过来的至少一条拉消息请求*/
                        if ((this.byteBufferRead.position() - this.processPosition) >= 8) {
                            /*pos 表示 byteBufferRead 可读数据中，最后一个帧数据（前面的帧不要了？）*/
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);
                            /*读取最后一帧数据，slave端当前的同步进度信息（slave的最大消息物理偏移量）*/
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            /*更新处理位点*/
                            this.processPosition = pos;

                            /*保存slave端的同步进度信息*/
                            HAConnection.this.slaveAckOffset = readOffset;

                            /*条件成立：slaveRequestOffset== -1 ，这个时候是给slaveRequestOffset 赋值的逻辑，
                            * slaveRequestOffset 在哪里会用到？ 在写数据服务时
                            * */
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }

                            /*唤醒阻塞的”生产者线程“*/
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) {
                        /*case2：加载失败，读缓冲区没有数据可以加载...*/
                        if (++readSizeZeroTimes >= 3) {
                            /*一般都是从这里跳出循环*/
                            break;
                        }
                    } else {
                        /*case3:socket处于半关闭状态，需要上层对象关闭HAConnection连接对象*/
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * 该类主要负责将消息内容传输给slave节点
     */
    class WriteSocketService extends ServiceThread {
        /*多路复用器*/
        private final Selector selector;
        /*master与slave 之间的 会话socketChannel*/
        private final SocketChannel socketChannel;

        /*master与 slave传输的数据格式：
         * {[phyOffset][size][data...]}{[phyOffset][size][data...]}{[phyOffset][size][data...]}
         * phyOffset:数据区间的开始偏移量，并不表示某一条具体的消息，表示的数据块开始的偏移量位置
         * size：同步的数据块大小
         * data：数据块 最大32kb，可能包含多条消息的数据
         * */

        /*协议头大小：12*/
        private final int headerSize = 8 + 4;
        /*帧头的缓冲区*/
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(headerSize);
        /*下一次给slave节点传输数据的位置信息，非常重要！（master需要知道 给当前slave节点同步的 位点）*/
        private long nextTransferFromWhere = -1;
        /*mappedFile的查询封装对象*/
        private SelectMappedBufferResult selectMappedBufferResult;
        /*上一轮同步数据是否传输完毕*/
        private boolean lastWriteOver = true;
        /*上次写数据时间戳*/
        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            /*socketChannel注册到多路复用器，关注”OP_WRITE“事件*/
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                try {
                    /*多路复用器最长阻塞1秒钟*/
                    this.selector.select(1000);
                    /*1.socket写缓冲区 有空间可写了 2.阻塞超时*/
                    /*条件成立：说明master节点还没收到slave节点的拉请求，放弃本次消息处理*/
                    if (-1 == HAConnection.this.slaveRequestOffset) {
                        /*休眠一段时间*/
                        Thread.sleep(10);
                        continue;
                    }
                    /*如果是第一次开始同步数据，需要计算需要传输的开始物理偏移量*/
                    if (-1 == this.nextTransferFromWhere) {
                        /*条件成立：说明slave是一个新的节点，从master正在顺序写的mappedFile的写入位点开始同步数据*/
                        if (0 == HAConnection.this.slaveRequestOffset) {
                            /*获取master最大消息物理偏移量*/
                            long masterOffset = HAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                            /*计算 最大消息物理偏移量 归属的mappedFile文件的开始偏移量*/
                            masterOffset =
                                masterOffset
                                    - (masterOffset % HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                    .getMappedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            /**/
                            this.nextTransferFromWhere = masterOffset;
                        } else {
                            /*一般从这里赋值*/
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + HAConnection.this.clientAddr
                            + "], and slave request " + HAConnection.this.slaveRequestOffset);
                    }

                    /*条件成立：上一轮 待发送数据 同步发送完成*/
                    if (this.lastWriteOver) {
                        /*获取当前系统时间与上次写入时间的时间间隔*/
                        long interval =
                            HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;

                        /*条件成立：当前系统是与上次最后写入的时间间隔大于HA心跳检测时间，需要发送一个心跳包（长度12字节，header数据包：物理偏移量+size）*/
                        if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaSendHeartbeatInterval()) {
                            /*发送一个header数据包  充当心跳信息 维持长连接*/
                            // Build Header
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(headerSize);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();

                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }
                    } else {
                        /*上一轮 待发送数据 同步未发送完成*/
                        /*就继续传输上一次数据，然后判断是否传输完成，如果还是没有传输完成，就结束本次事件处理，
                        * 待下次事件到达时，继续将未传输完的数据写入到slave节点
                        * */
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }

                    /*到 commitLog查找 nextTransferFromWhere开始的有效数据*/
                    SelectMappedBufferResult selectResult =
                        HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
                    if (selectResult != null) {
                        /*查询到消息*/
                        int size = selectResult.getSize();
                        /*有效数据超过32k， 重置数据大小为32k*/
                        if (size > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            size = HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        long thisOffset = this.nextTransferFromWhere;
                        /*增加size，下一轮传输 跳过本帧数据*/
                        this.nextTransferFromWhere += size;
                        /*设置本次 帧数据传输大小*/
                        selectResult.getByteBuffer().limit(size);

                        this.selectMappedBufferResult = selectResult;

                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(headerSize);
                        this.byteBufferHeader.putLong(thisOffset);
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();

                        /*lastWriteOver：true 表示上一轮数据处理完成*/
                        this.lastWriteOver = this.transferData();
                    } else {
                        /*如果没查到匹配的消息，通知所有等待的线程等待100毫秒*/
                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }
            HAConnection.this.haService.getWaitNotifyObject().removeFromWaitingThreadTable();
            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }
            this.makeStop();
            readSocketService.makeStop();
            haService.removeConnection(HAConnection.this);
            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }
            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
            HAConnection.log.info(this.getServiceName() + " service end");
        }

        /**
         * 同步数据到slave节点
         * 返回boolean值：
         * true：表示本轮数据同步完成（header+body数据）
         * false：表示本轮同步未完成（header | body数据 其中一个未同步完成，都会返回false）
         * @return
         * @throws Exception
         */
        private boolean transferData() throws Exception {
            /*控制循环，写失败连续3次，退出循环*/
            int writeSizeZeroTimes = 0;
            /*先传输帧头，再传输帧body*/
            // Write Header
            /*条件成立：说明 byteBufferHeader里面有可读取的数据*/
            while (this.byteBufferHeader.hasRemaining()) {
                /*向socketChannel写数据，返回写成功的数据量*/
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    /*case1：写成功*/
                    writeSizeZeroTimes = 0;
                    /*更新上次写数据时间戳*/
                    this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    /*case2：写失败*/
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }

            /*selectMappedBufferResult保存的是 本轮待传输的数据*/
            if (null == this.selectMappedBufferResult) {/*条件成立：心跳包*/
                /*判断心跳包 是否 全部发送完成*/
                return !this.byteBufferHeader.hasRemaining();
            }

            /*归0*/
            writeSizeZeroTimes = 0;

            // Write Body 写body数据
            if (!this.byteBufferHeader.hasRemaining()) {/*只有header数据写成功之后，才会写body数据*/
                /*条件成立：说明 selectMappedBufferResult 里面有待读取的数据*/
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    /*向socketChannel写数据*/
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        /*case1：写成功，并不代表 smbr里面的数据 全部都写成功*/
                        writeSizeZeroTimes = 0;
                        /*更新上一次写数据时间*/
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        /*case2：写失败，因为socket缓冲区写满了*/
                        if (++writeSizeZeroTimes >= 3) {
                            /*重试超过三次，退出循环*/
                            break;
                        }
                    }/**/ else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            /*true：表示本轮数据同步完成（header+body）false：表示本轮同步未完成（header | body 其中一个未同步完成，都会返回false）*/
            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            /*条件成立：本轮smbr里面的数据都同步完成了*/
            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                /*释放里面对应的mappedFile的引用计数*/
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }
}
