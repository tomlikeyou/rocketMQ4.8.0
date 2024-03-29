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
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

public class MappedFile extends ReferenceResource {
    /*内存页大小：4k*/
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /*当前进程下 所有的mappedFile 占用的 总虚拟内存大小*/
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);
    /*当前进程下 所有的mappedFile的个数*/
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    /*当前mappedFile 数据写入点*/
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    /*当前mappedFile的数据落盘位点(flushedPosition之前的数据 都是安全数据， flushedPosition ~wrotePosition之间的数据 属于脏页数据)*/
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    /*文件大小*/
    protected int fileSize;
    /*文件通道*/
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     * 堆外内存，如果不为空，数据会先保存到这里，然后再保存到 MappedFile创建的fileChannel通道中
     */
    protected ByteBuffer writeBuffer = null;
    /*堆外内存池*/
    protected TransientStorePool transientStorePool = null;
    /*文件名 （commitLog、ConsumeQueue：文件名就是 第一条消息的 物理偏移量 索引文件：年月日小时分钟秒...）*/
    private String fileName;
    /*文件名转long*/
    private long fileFromOffset;
    /*文件对象*/
    private File file;
    /*内存映射缓冲区，访问虚拟内存*/
    private MappedByteBuffer mappedByteBuffer;
    /*该文件下 保存的第一条消息 msg的存储时间*/
    private volatile long storeTimestamp = 0;
    /*当前文件如果是 目录内 有效文件的 首文件/第一个文件的话，该值为true*/
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    /*
    * 参数1：文件名称（绝对路径文件名）
    * 参数2：文件大小
    * */
    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    /**
     *
     * @param fileName 文件名称（绝对路径文件名称）
     * @param fileSize 文件大小
     * @param transientStorePool 临时堆外内存池（存放临时的数据，如果开启了堆外内存池，数据会先写入到堆外内存，
     *                           再通过 commit 定时线程将数据复制到与目标物理文件对应的内存映射中。
     *                           RocketMQ 引入该机制主要的原因是提供一种内存锁定，让内存一直被RocketMQ 占用，不会被交换至磁盘中。）
     * @throws IOException
     */
    public MappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    public void init(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        /*保存写缓冲区*/
        this.writeBuffer = transientStorePool.borrowBuffer();
        /*保存堆外内存池*/
        this.transientStorePool = transientStorePool;
    }

    /*
     * 参数1：文件名称（绝对路径文件名）
     * 参数2：文件大小
     * */
    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        /*创建文件对象*/
        this.file = new File(fileName);
        /*文件名转long*/
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        ensureDirOK(this.file.getParent());

        try {
            /*创建文件通道（jdk nio）*/
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            /*获取文件内存映射缓冲区*/
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    /**
     *
     * @param msg 消息
     * @param cb 追加消息回调（消息哪些个字段 需要追加到 文件中，都由它控制）
     * @return
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;
        /*获取 当前内存映射文件的写入位点*/
        int currentPos = this.wrotePosition.get();
        /*条件成立：说明文件还可以继续写*/
        if (currentPos < this.fileSize) {
            /*使用内存映射buffer  创建 切片*/
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            /*将切片byteBuffer 的position设置为 数据写入位点*/
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            if (messageExt instanceof MessageExtBrokerInner) {
                /*向内存映射中追加数据*/
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            /*更新数据写入位点，加上刚刚写入的数据量*/
            this.wrotePosition.addAndGet(result.getWroteBytes());
            /*保存一下文件最后一条msg的存储时间*/
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    /**
     *
     * @param data 需要写入到 文件的字节数组
     * @return boolean true 写入成功 反之失败
     */
    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();
        /*条件成立：说明当前 文件剩余空间 可以容下 data数组*/
        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            /*更新写入数据的位点信息*/
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }


    /**
     *
     * @param flushLeastPages 刷盘的最小页数（等于0时，属于强制刷盘，>0时，需要脏页数据 达到 flushLeastPages 才进行物理刷盘）
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
        /*条件成立：说明可以进行刷盘*/
        if (this.isAbleToFlush(flushLeastPages)) {
            /*引用计数自增，保证刷盘过程中 不会释放资源*/
            if (this.hold()) {
                /*获取 数据写入位点*/
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        /*刷盘*/
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }
                /*数据写入位点赋值给 刷盘位点*/
                this.flushedPosition.set(value);
                /*引用计数减 1*/
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        /*返回最新数据的刷盘点*/
        return this.getFlushedPosition();
    }

    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    protected void commit0(final int commitLeastPages) {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - lastCommittedPosition > commitLeastPages) {
            try {
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    /**
     *
     * @param flushLeastPages 刷盘的最小页数（等于0时，属于强制刷盘，>0时，需要脏页数据 达到 flushLeastPages 才进行物理刷盘）
     * @return
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        /*获取当前刷盘位点*/
        int flush = this.flushedPosition.get();
        /*获取当前写入位点*/
        int write = getReadPosition();
        /*条件成立：文件写满 这种情况 一定返回true 执行刷盘操作*/
        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }
        /*执行到这里：说明 flushLeastPages == 0*/
        /*只要有脏数据，就刷盘*/
        return write > flush;
    }

    protected boolean isAbleToCommit(final int commitLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    /**
     * 该方法以 pos为开始位点，到有效数据位置 创建一个切片 ByteBuffer，供业务访问数据
     * @param pos 数据起始位点
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        /*获取 有效数据位点（wrotePosition）*/
        int readPosition = getReadPosition();
        /*条件成立：说明pos 处于有效数据区间*/
        if (pos < readPosition && pos >= 0) {
            /*资源引用计数 + 1 */
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }
        /*释放堆外内存*/
        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /*删除文件时 使用的方法*/
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                /*关闭文件通道*/
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                /*删除文件*/
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeElapsedTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    /**
     * 内存映射文件 内存预热
     * @param type
     * @param pages
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
