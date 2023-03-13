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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageStatus;

public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /*master节点 当前有多少个 slave节点 与其进行数据同步*/
    private final AtomicInteger connectionCount = new AtomicInteger(0);

    /*master节点 会给每个向其发起 连接的 slave节点（socketChannel）创建一个 HAConnection
    它封装了 socketChannel，控制master端向slave端传输数据的逻辑*/
    private final List<HAConnection> connectionList = new LinkedList<>();

    /*master启动之后 绑定服务端指定端口， 监听slave节点的连接 acceptSocketService 封装了这块逻辑
    * HA这块的逻辑 并没有跟netty 那套逻辑混淆在一起，而是使用 原生的NIO
    * */
    private final AcceptSocketService acceptSocketService;

    /*存储主模块*/
    private final DefaultMessageStore defaultMessageStore;

    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    /*maste向 slave节点推送的 最大offset（表示数据同步进度吧）*/
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    /*前面讲的 GroupCommitService 没太大区别 主要也是控制：生产者线程 阻塞等待的逻辑*/
    private final GroupTransferService groupTransferService;

    /*slave节点的客户端对象，slave端才会正常运行该实例*/
    private final HAClient haClient;

    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService =
            new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        this.haClient = new HAClient();
    }

    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
            result
                && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    public void notifyTransferSome(final long offset) {
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }

    public void start() throws Exception {
        this.acceptSocketService.beginAccept();
        this.acceptSocketService.start();
        this.groupTransferService.start();
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * Listens to slave connections to create {@link HAConnection}.
     */
    class AcceptSocketService extends ServiceThread {
        /*master 绑定监听的端口信息*/
        private final SocketAddress socketAddressListen;
        /*服务器端nio通道*/
        private ServerSocketChannel serverSocketChannel;
        private Selector selector;

        public AcceptSocketService(final int port) {
            /*port：10912*/
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            /*获取服务端socketChannel*/
            this.serverSocketChannel = ServerSocketChannel.open();
            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);
            /*绑定端口*/
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            /*设置为非阻塞*/
            this.serverSocketChannel.configureBlocking(false);
            /*socketChannel注册到多路复用器上，关注 OP_ACCEPT事件*/
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    /*多路复用器阻塞 最长1秒钟*/
                    this.selector.select(1000);

                    /*几种情况执行到这里：1.事件就绪（OP_ACCPET）2.超时*/
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                /*OP_ACCEPT事件 就绪*/

                                /*获取到客户端的连接*/
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                        /*为每个连接master服务器的 slave socketChannel 封装一个 HAConnection对象*/
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        /*启动 HAConnection 对象（启动内部的两个服务：读数据服务  写数据服务）*/
                                        conn.start();
                                        /*加入到 集合内*/
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * GroupTransferService Service
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();
        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();

        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            this.wakeup();
        }

        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        private void swapRequests() {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        private void doWaitTransfer() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        long waitUntilWhen = HAService.this.defaultMessageStore.getSystemClock().now()
                            + HAService.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout();
                        while (!transferOK && HAService.this.defaultMessageStore.getSystemClock().now() < waitUntilWhen) {
                            this.notifyTransferObject.waitForRunning(1000);
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        if (!transferOK) {
                            log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }

                        req.wakeupCustomer(transferOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }

                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    /**
     * slave端 运行的 HA客户端代码，它会和master服务器 建立长连接，上报本地同步进度，消费服务器发来的msg数据
     */
    class HAClient extends ServiceThread {
        /*4MB*/
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
        /*ip：port master节点启动时监听的 HA会话端口（和netty绑定的那个服务端口不是同一个）
        * 什么时候赋值的该字段？ slave节点会赋值 master节点不会赋值
        * */
        private final AtomicReference<String> masterAddress = new AtomicReference<>();
        /*因为底层通信使用的是 NIO 接口，所以所有的内容 都是 通过块传输的，所以上报 slave offset时 需要使用该 buffer*/
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        /*客户端与 master 的会话通道*/
        private SocketChannel socketChannel;
        private Selector selector;
        /*上次会话通信时间 用于控制 socketChannel 是否关闭的*/
        private long lastWriteTimestamp = System.currentTimeMillis();

        /*slave当前的进度信息*/
        private long currentReportedOffset = 0;
        private int dispatchPosition = 0;

        /*master与 slave传输的数据格式：
        * {[phyOffset][size][data...]}{[phyOffset][size][data...]}{[phyOffset][size][data...]}
        * phyOffset:数据区间的开始偏移量，并不表示某一条具体的消息，表示的数据块开始的偏移量位置
        * size：同步的数据块大小
        * data：数据块 最大32kb，可能包含多条消息的数据
        * */

        /*byteBuffer 用于到 socket读缓冲区 加载 就绪的数据使用 4mb*/

        /*byteBuffer 加载完之后，做什么事？ 基于 帧协议去解析，解析出来的帧数据，然后存储到slave的 commitLog中
        * 处理过程中 程序并没有去调整 byteBufferRead（调整过，但是解析完一条数据之后 又给恢复 原来的position）position指针。
        * 总之，byteBufferRead会遇到 pos ==limit清空，这种情况下，最后一条帧数据 大概率 是半包数据，
        * 半包数据，程序不能把他给丢掉，就将该半包数据（不完整的帧数据） 拷贝到byteBufferBackup这个缓冲区，然后将 byteBufferRead clean（pos指针设置为0）
        * swap交换 byteBufferBackup 成为 byteBufferRead，原byteBufferRead成为 byteBufferBackup
        * 再使用 byteBufferRead（包含了半包数据...），到socketChannel读缓冲区 继续加载剩余数据 然后程序就会继续处理了
        *
        * 说了这么多，其实就是处理半包数据的问题
        * */
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        /**
         * slave节点 该方法才会被调用到，传递给 master节点暴露的 ha地址端口信息
         * master节点 该方法是永远不会被调用到的，也就是说 master节点的 haClient对象的 masterAddress值 为空值
         * @param newAddr
         */
        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        private boolean isTimeToReportOffset() {
            long interval =
                HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                .getHaSendHeartbeatInterval();

            return needHeart;
        }

        /**
         * 上报slave同步进度
         * @param maxOffset
         * @return
         */
        private boolean reportSlaveMaxOffset(final long maxOffset) {
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            /*slave端的*/
            this.reportOffset.putLong(maxOffset);
            /*position归0*/
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            /*大概率一次就成功了，8个字节*/
            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
            /*写成功之后，pos ==limit 返回true*/
            return !this.reportOffset.hasRemaining();
        }

        private void reallocateByteBuffer() {
            /*remain 表示 byteBufferRead 尚未处理的字节数量*/
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
            /*条件成立：说明 byteBufferRead 最后一帧数据 是一个半包数据（不完整的数据）*/
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPosition);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                /*半包数据拷贝到 byteBufferBackup*/
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            /*交换两个byteBuffer*/
            this.swapByteBuffer();

            /*设置pos为 remain，后续加载数据时，pos从remain开始向后移动*/
            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            /*因为 当前 byteBufferRead交换之后，它相当于是一个全新的 byteBuffer了*/
            this.dispatchPosition = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        /**
         * 处理 master 发送给slave 数据的逻辑
         * 返回布尔值：true表示处理成功，false表示socket 处于半关闭状态，需要上层重建 haClient
         * @return
         */
        private boolean processReadEvent() {
            /*控制循环的一个变量，值为3时 退出循环*/
            int readSizeZeroTimes = 0;
            /*循环条件：byteBufferRead 有空闲空间可以去socket读缓冲区加载 数据...一般条件都成立*/
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    /*到socket加载数据 到 byteBufferRead */
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        /*case1:加载成功 有新数据*/
                        /*归0*/
                        readSizeZeroTimes = 0;

                        /*处理master发送给slave的数据的逻辑*/
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) {
                        /*case2：无新数据*/
                        if (++readSizeZeroTimes >= 3) {
                            /*正常从这里跳出循环*/
                            break;
                        }
                    } else {
                        /*readSize==-1 表示socket处于半关闭状态（对端关闭了）*/
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        private boolean dispatchReadRequest() {
            /*协议头大小：12*/
            final int msgHeaderSize = 8 + 4; // phyoffset + size
            /*该变量记录 byteBufferRead 处理数据之前，position值，用于处理完 数据之后 恢复 position指针！*/
            int readSocketPos = this.byteBufferRead.position();

            while (true) {
                /*diff表示 当前 byteBufferRead 还剩余多少byte 没有处理（每处理完一条帧数据，都会更新dispatchPosition，让它加一帧数据长度）*/
                int diff = this.byteBufferRead.position() - this.dispatchPosition;
                /*条件成立：byteBufferRead 内部最起码是有一个 完整的header数据的 */
                if (diff >= msgHeaderSize) {
                    /*读取header*/
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);

                    /*slave端最大物理偏移量*/
                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    if (slavePhyOffset != 0) {
                        /*正常情况 slavePhyOffset == masterPhyOffset两者相等的，不存在不相等的情况...
                        * 一帧一帧 同步的，怎么会出现问题呢？
                        * */
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }

                    /*条件成立： byteBufferRead 内部最起码 包含当前帧的全部数据的*/
                    if (diff >= (msgHeaderSize + bodySize)) {
                        /*处理帧数据*/
                        /*创建一个bodySize大小的字节数组，用于提取 帧内的 body数据*/
                        byte[] bodyData = new byte[bodySize];
                        /*设置pos 为当前帧的 body 起始位置*/
                        this.byteBufferRead.position(this.dispatchPosition + msgHeaderSize);
                        /*读取数据到字节数组里*/
                        this.byteBufferRead.get(bodyData);

                        /*slave存储数据的逻辑
                        * 这里为啥不做任何校验了，向新msg插入一样？
                        * 没必要了，这些数据都是从master的commitLog中拿来的，在master存msg时候，该校验的都校验过了
                        * */
                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);

                        /*恢复 byteBufferRead 的pos指针*/
                        this.byteBufferRead.position(readSocketPos);
                        /*加一帧数据长度，方便处理下一条数据时使用*/
                        this.dispatchPosition += msgHeaderSize + bodySize;

                        /*上报slave的同步进度信息*/
                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        /*当 diff >= msgHeaderSize 与 diff >= (msgHeaderSize + bodySize) 都成立，会continue，继续处理后面帧数据*/
                        continue;
                    }
                }

                /*当 diff >= msgHeaderSize 不成立 或者 diff >= (msgHeaderSize + bodySize)不成立，就会执行下面的代码*/

                /*条件成立：说明 byteBufferRead 写满了*/
                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }

        private boolean connectMaster() throws ClosedChannelException {
            if (null == socketChannel) {
                /*master节点 暴露的 HA地址信息*/
                String addr = this.masterAddress.get();
                /*slave节点addr 才不会为null，master节点 这里是获取不到值的*/
                if (addr != null) {
                    /*封装地址信息的对象*/
                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        /*nio方式建立连接*/
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            /*注册到 “多路复用器”，关注“OP_READ” 事件*/
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }

                /*初始化 上报进度字段 为slave节点的 commitLog最大消息物理偏移量*/
                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            return this.socketChannel != null;
        }

        private void closeMaster() {
            if (null != this.socketChannel) {
                try {

                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                this.lastWriteTimestamp = 0;
                this.dispatchPosition = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    /*true：slave节点连接成功master才会返回true
                    * false：
                    * 1.master节点该实例运行时 因为 masterAddr是空，所以一定会返回false。
                    * 2.slave连接 master失败
                    * */
                    if (this.connectMaster()) {

                        /*slave每5秒 一定上报一次 slave端的 同步进度信息给 master*/
                        if (this.isTimeToReportOffset()) {
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {
                                this.closeMaster();
                            }
                        }


                        this.selector.select(1000);

                        /*执行到这里：有两种情况
                        * 1.socketChannel OP_READ就绪
                        * 2.select方法超时
                        * */
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }

                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }
        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}
