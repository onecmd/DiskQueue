package com.onecmd.diskqueue;

import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class CacheConfig {

    private String diskCacheFileRoot = "/var/diskqueue/data";

    private int capacity = 500000;
    private int heapCapacity = 15000;
    private long maxDiskSize = 5120*1048576; // MB=1048576
    private int pageSize = 1000;
    private boolean usingDisk = true;
    private int persistTimeoutSeconds = 10;

    private AtomicLong subPoolId = new AtomicLong(0);

    public long getNewSubPoolId(){
        return subPoolId.incrementAndGet();
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public int getHeapCapacity() {
        return heapCapacity;
    }

    public void setHeapCapacity(int heapCapacity) {
        this.heapCapacity = heapCapacity;
    }

    public long getMaxDiskSize() {
        return maxDiskSize;
    }

    public void setMaxDiskSize(long maxDiskSize) {
        this.maxDiskSize = maxDiskSize;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int fileSize) {
        this.pageSize = fileSize;
    }

    public boolean isUsingDisk() {
        return usingDisk;
    }

    public void setUsingDisk(boolean usingDisk) {
        this.usingDisk = usingDisk;
    }

    public void setDiskCacheFileRoot(String diskCacheFolder) {
        this.diskCacheFileRoot = diskCacheFolder;
    }

    public String getDiskCacheFileRoot(){
        return diskCacheFileRoot;
    }

    public int getPersistTimeoutSeconds() {
        return persistTimeoutSeconds;
    }

    public void setPersistTimeoutSeconds(int persistTimeoutSeconds) {
        this.persistTimeoutSeconds = persistTimeoutSeconds;
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();

        sb.append("capacity="+capacity);
        sb.append(", heapCapacity="+heapCapacity);
        sb.append(", maxDiskSize="+maxDiskSize);
        sb.append(", pageSize="+pageSize);
        sb.append(", usingDisk="+usingDisk);
        sb.append(", persistTimeout="+persistTimeoutSeconds);

        return sb.toString();
    }
}
