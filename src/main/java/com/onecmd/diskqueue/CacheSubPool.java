package com.onecmd.diskqueue;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 */
public class CacheSubPool<T> {

    private static Logger LOGGER = LoggerFactory.getLogger(CacheSubPool.class);

    private long id = 0;
    private String diskCacheFileRoot;
    private CacheConfig config;
    private CacheStatistics statistics;
    private Class<T> objectType = null;

    private LinkedBlockingDeque<T> bufferQueue = new LinkedBlockingDeque<T>();
    private AtomicInteger size = new AtomicInteger(0);
    private AtomicInteger sizeInDisk = new AtomicInteger(0);

    private boolean persisted = false;

    private Lock putLock = new ReentrantLock();
    private Lock takeLock = new ReentrantLock();

    private void fullLock(){
        takeLock.lock();
        putLock.lock();
    }

    private void fullUnLock(){
        takeLock.unlock();
        putLock.unlock();
    }
    private CachePersiter<T> cachePersiter = new JsonFilePersister<T>();

    public CacheSubPool(long id, CacheConfig config, CacheStatistics statistics, Class<T> objectType){
        this.id = id;
        this.diskCacheFileRoot = config.getDiskCacheFileRoot();
        this.config = config;
        this.statistics = statistics;
        this.objectType = objectType;

        this.persisted = false;
    }

    protected void setCachePersiter(CachePersiter<T> cachePersiter){
        this.cachePersiter = cachePersiter;
    }

    public String getFilePath(){
        return diskCacheFileRoot+File.separator+id+".dat";
    }

    public int getHeapSize(){
        return  size.get() - sizeInDisk.get();
    }

    public int size(){
        return size.get();
    }

    public int getSizeInDisk(){
        return sizeInDisk.get();
    }

    public int getAndAddSizeInDisk(int value){
        int prev = sizeInDisk.getAndAdd(value);
        statistics.getAndAddDiskSize(value);

        return prev;
    }

    public long getId(){
        return id;
    }

    public boolean isFull(){
        return bufferQueue.size() >= config.getPageSize();
    }

    public void add(T obj) throws Exception {
        LOGGER.trace("Enter subPool[" + id + "].add()");
        try {
            putLock.lock();

            bufferQueue.add(obj);
        }
        finally {
            putLock.unlock();
        }

        size.getAndIncrement();
    }

    public int drainTo(Collection<T> list, int fetchSize) throws Exception {
        LOGGER.trace("Enter subPool["+id+"].drainTo(prevSize="+list.size()+")");

        try {
            takeLock.lock();

            loadToHeap();

            int total = bufferQueue.drainTo(list, fetchSize);
            size.getAndAdd(-1 * total);

            return total;
        }
        finally {
            takeLock.unlock();
        }
    }

    public void clear() {
        LOGGER.trace("Enter subPool[" + id + "].clear()");

        try {
            fullLock();

            bufferQueue.clear();
            size.set(bufferQueue.size());

            getAndAddSizeInDisk(-1 * sizeInDisk.get());

            persisted = false;

            File file = new File(getFilePath());
            if (file.exists()) {
                statistics.getAndAddDiskFileSize(-1 * file.length());
                FileUtils.deleteQuietly(file);
            }
        }
        finally {
            fullUnLock();
        }
    }

    /**
     * Throw exception if load failed
     * @throws Exception
     */
    private void loadToHeap() throws Exception {
        LOGGER.trace("Enter subPool["+id+"].loadToHeap()");

        if (!persisted) {
            return;
        }

        File file = new File(getFilePath());
        if (!file.exists()) {
            throw new Exception("File damaged or not exist.");
        } else { // persisted==true && file.exists():
            long fileSize = file.length();
            List<T> diskCaches = retryReadDataToFile(file);
            FileUtils.deleteQuietly(file);

            LOGGER.trace("subPool[" + id + "]: file exist, loaded from disk: " + diskCaches.size());
            addListToQueueHead(bufferQueue, diskCaches);

            getAndAddSizeInDisk(-1 * diskCaches.size());
            statistics.getAndAddLoadedFiles(1);
            statistics.getAndAddDiskFileSize(-1 * fileSize);

            persisted = false;
        }
    }

    private List<T> retryReadDataToFile(File file) throws Exception {
        return cachePersiter.read(file, objectType);
    }

    /**
     * Success: return wrote numbers of objects(if no need persist also return 0)
     * Load from disk failed: throw exceptions
     * Persist failed: return 0;
     *
     * @return
     * @throws Exception
     */
    public int persist() throws Exception {
        LOGGER.trace("Enter subPool["+id+"].persist(queueSize: "+bufferQueue.size()+")");

        try {
            fullLock();

            loadToHeap();

            if (bufferQueue.size() < 1) {
                return 0;
            }

            int total = 0;
            try {
                total = retryNewIdToWriteDataToFile(bufferQueue);
                LOGGER.trace("subPool["+id+"]: wrote objects to file: "+total);
                persisted = true;
            } catch (Exception e) {
                LOGGER.warn("Failed to persist cache: " + e.getMessage(), e);
            }

            return total;
        }
        finally {
            fullUnLock();
        }
    }

    /**
     * Success: return wrote numbers of objects
     * Failed: throw exceptions
     * @param queue
     * @return
     * @throws Exception
     */
    private int retryNewIdToWriteDataToFile(BlockingQueue<T> queue) throws Exception {
        int triedTimes = 3;
        IOException exception = null;
        while ((triedTimes --) >0) {
            try {
                File file = new File(getFilePath());
                int total = retryWriteDataToFile(file, bufferQueue);
                return total;
            }
            catch (IOException e){
                refreshId();
                exception = e;
            }
        }

        throw exception;
    }

    private void refreshId(){
        long prevId = id;
        this.id = config.getNewSubPoolId();
        LOGGER.trace("SubPool[" + prevId + "] renamed to new ID: " + id);
    }

    /**
     * Success: return wrote numbers of objects
     * Failed: throw exceptions
     * @param file
     * @param queue
     * @return
     * @throws Exception
     */
    private int retryWriteDataToFile(File file, LinkedBlockingDeque<T> queue) throws Exception {

            ArrayList<T> caches = new ArrayList<T>();
            queue.drainTo(caches);

            try {
                long fileSize = cachePersiter.write(file, caches);
                getAndAddSizeInDisk(caches.size());
                statistics.getAndAddDiskFileSize(fileSize);
                statistics.getAndAddPersistedFiles(1);
                return caches.size();
            }
            catch (Exception e){
                addListToQueueHead(queue, caches);
                throw e;
            }
    }

    protected void addListToQueueHead(LinkedBlockingDeque<T> queue, List<T> caches){
        LOGGER.trace("Enter subPool["+id+"].addListToPoolHead(size: "+caches.size()+")");
        if(caches.size() < 1) {
            return;
        }

        for (int i=caches.size()-1; i>=0; i--){
            queue.addFirst(caches.get(i));
        }
    }
}
