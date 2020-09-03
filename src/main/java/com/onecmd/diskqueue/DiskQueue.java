package com.onecmd.diskqueue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 */
public class DiskQueue<T> {

    private static Logger LOGGER = LoggerFactory.getLogger(DiskQueue.class);

    private String name = "DiskQueue";

    private ConcurrentLinkedDeque<CacheSubPool<T>> inQueue = new ConcurrentLinkedDeque<>();

    private Class<T> objectType = null;

    private CacheStatistics statistics;
    private CacheConfig config;

    private ExecutorService persistThread = Executors.newSingleThreadExecutor();

    private Lock takeLock = new ReentrantLock();
    private Lock putLock = new ReentrantLock();

    private void fullLock(){
        takeLock.lock();
        putLock.lock();
    }

    private void fullUnLock(){
        takeLock.unlock();
        putLock.unlock();
    }

    public CacheConfig getConfig() {
        return config;
    }

    public CacheStatistics getStatistics() {
        return statistics;
    }

    public long getNewSubPoolId(){
        return config.getNewSubPoolId();
    }

    public DiskQueue(String name, Class<T> objectType, int capacity, int heapCapacity, long maxDiskSizeInMB, int filePageSize, boolean usingDisk, int persistTimeoutSeconds, String diskCacheFileRoot)  {
        this.name = name;
        statistics = new CacheStatistics();

        config = new CacheConfig();

        config.setCapacity(capacity < 100 ? 100 : capacity);

        int heap = heapCapacity> capacity ? capacity : heapCapacity;
        config.setHeapCapacity(heap < 1 ? 0 : heap);

        long maxDiskSize = maxDiskSizeInMB < 1 ? config.getMaxDiskSize() : maxDiskSizeInMB*1024*1024;
        config.setMaxDiskSize(maxDiskSize);

        int pageSize = filePageSize > heapCapacity/3 ? heapCapacity/3 : filePageSize;
        config.setPageSize(pageSize);

        config.setUsingDisk(usingDisk);

        config.setDiskCacheFileRoot(diskCacheFileRoot + File.separator + name);
        this.objectType = objectType;

        config.setPersistTimeoutSeconds(persistTimeoutSeconds);

        initDiskStorage();

        CacheSubPool<T> subPool = this.createCacheSubPool();
        inQueue.addLast(subPool);

    }

    private CacheSubPool<T> createCacheSubPool(){
        CacheSubPool<T> subPool = new CacheSubPool<T>(getNewSubPoolId(), config, statistics, objectType);
        return subPool;
    }

    public void initDiskStorage() {
        if(config.isUsingDisk()){
            File file = new File(config.getDiskCacheFileRoot());
            LOGGER.info("Disk data file path: " + file.getAbsolutePath());

            FileUtils.deleteQuietly(file);

            file.mkdirs();
            config.setDiskCacheFileRoot(file.getAbsolutePath());

            startMonitoringThread();
        }
    }

    public void startMonitoringThread(){
        new Thread(){
            public void run(){
                long lastPrintTime = 0;
                while (true){
                    if(System.currentTimeMillis() - lastPrintTime >10000) {
                        LOGGER.info("Configuration: " + config.toString());
                        LOGGER.info("Statistics: " + statistics.toString());
                        lastPrintTime = System.currentTimeMillis();
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOGGER.warn("System exit: "+e.getMessage());
                        break;
                    }
                }
            }
        }.start();
    }

    public int size(){
        return statistics.getCacheSize();
    }

    public int getHeapSize(){
        return statistics.getHeapSize();
    }

    public boolean offer(T obj){
        return add(obj);
    }

    public boolean add(T obj){
        LOGGER.trace("Enter add()");
        if(null == obj){
            throw new NullPointerException("Parameter obj should not be NULL.");
        }

        try {
            putLock.lock();

            if(size() >= config.getCapacity()){
                LOGGER.warn("Failed to add ["+getObjectStr(obj)+"] to cache[name="+name+"]: queue full: capacity=" + config.getCapacity() + ", size: " + size());
                return false;
            }
            else if(isDiskFull()){
                LOGGER.warn("Failed to add ["+getObjectStr(obj)+"] to cache[name="+name+"]: cache disk full: MaxDiskSize=" + config.getMaxDiskSize() + ", fileSize: " + statistics.getDiskFileSize());
                return false;
            }

            statistics.getAndAddCacheSize(1);
            checkAndPersist();

            if(getHeapSize() > config.getHeapCapacity()){
                throw new Exception("Failed to persist heap data to file: HeapCapacity=" + config.getHeapCapacity() + ", heapSize=" + getHeapSize() + ".");
            }
            else {
                CacheSubPool<T> entry = getInsertCacheEntry();
                entry.add(obj);
            }

            return true;
        } catch (Exception e) {
            LOGGER.error("Failed to add [" + getObjectStr(obj) + "] to cache[name="+name+"]: " + e.getMessage(), e);
            statistics.getAndAddCacheSize(-1);
            return false;
        }
        finally {
            putLock.unlock();
        }
    }

    private String getObjectStr(T obj) {
        try {
            return new ObjectMapper().writeValueAsString(obj);
        } catch (Exception e) {
            LOGGER.error("Failed to parse object to String: " + obj, e);
            return "WRONG_FORMAT";
        }
    }

    private boolean isLessThen2Capacity(){
        return getHeapSize()+2 >= config.getHeapCapacity();
    }

    private void checkAndPersist() throws Exception {
        if(getHeapSize() < config.getHeapCapacity() ){
            return;
        }

        Iterator<CacheSubPool<T>> iter = inQueue.descendingIterator();
        if(!iter.hasNext()){
            return;
        }
        try {
            CacheSubPool<T> head = iter.next();

            while (iter.hasNext() && isLessThen2Capacity()) {
                if (isDiskFull()) {
                    throw new Exception("Failed to persist heap data to file: disk full.");
                }

                CacheSubPool<T> pool = iter.next();
                if (pool != null && pool.isFull()) {
                    timeOutPersist(pool);
                }
            }
        }
        catch (NoSuchElementException e){
            LOGGER.error("Failed to persist cache data: " + e.getMessage(), e);
        }
    }

    private int timeOutPersist(final CacheSubPool<T> pool){
        Future<Integer> future = persistThread.submit(
                new Callable<Integer>() {
                    public Integer call() throws Exception {
                        return pool.persist();
                    }});

        int total = 0;
        try {
            total = future.get(config.getPersistTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (Exception e){
            LOGGER.warn("Failed to persist: " + e.getMessage(), e);
            future.cancel(true);
        }
        return total;
    }

    private CacheSubPool<T> getInsertCacheEntry(){
        CacheSubPool<T> subPool = null;

        synchronized (inQueue) {
            try {
                CacheSubPool<T> last = inQueue.getLast();
                if (last.isFull()) {
                    inQueue.addLast(createCacheSubPool());
                }
            } catch (NoSuchElementException e) {
                inQueue.addLast(createCacheSubPool());
            }
            subPool = inQueue.getLast();
        }

        return subPool;
    }

    /**
     * Success: return how much drained;
     * Failed: throw exception;
     * @param pool
     * @param fetchSize
     * @return
     */
    public int drainTo(List<T> pool, int fetchSize) throws Exception {
         if(null == pool){
            throw new NullPointerException("Parameter pool should not be NULL.");
        }
        LOGGER.trace("Exit drainTo(poolSize=" + pool.size() + ", fetchSize=" + fetchSize + ")");

        int total = 0;
        try {
            takeLock.lock();

            Iterator<CacheSubPool<T>> iter = inQueue.iterator();

            while (iter.hasNext() && total < fetchSize){
                CacheSubPool<T> subPool = iter.next();
                total += subPool.drainTo(pool, fetchSize);
                if(subPool.size()<=0 && iter.hasNext()){
                    iter.remove();
                }
            }
        }
        finally {
            takeLock.unlock();
        }

        statistics.getAndAddCacheSize(-1 * total);

        LOGGER.trace("Exit drainTo(fetchSize=" + fetchSize + ", drained=" + total + ")");
        return total;
    }

    public void clear() {
        LOGGER.trace("Enter clear()");
        try {
            fullLock();
            for (CacheSubPool<T> pool : inQueue) {
                pool.clear();
            }
            inQueue.clear();
        }
        finally {
            fullUnLock();
        }

        statistics.getAndAddCacheSize(-1 * statistics.getCacheSize());
    }

    public boolean isEmpty(){
        return size()<1;
    }

    public boolean isDiskFull(){
        return statistics.getDiskFileSize() >= config.getMaxDiskSize();
    }

    public String getSummary(){
        return config.toString()+", "+statistics.toString();
    }
}
