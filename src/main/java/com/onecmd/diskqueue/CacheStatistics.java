package com.onecmd.diskqueue;

import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class CacheStatistics {

    private AtomicInteger cacheSize = new AtomicInteger(0);
    private AtomicInteger heapSize = new AtomicInteger(0);
    private AtomicInteger diskSize = new AtomicInteger(0);
    private AtomicInteger diskFiles = new AtomicInteger(0);

    private AtomicLong persistedFiles = new AtomicLong(0);
    private AtomicLong loadedFiles = new AtomicLong(0);

    private AtomicLong diskFileSize = new AtomicLong(0);

    public int getCacheSize() {
        return cacheSize.get();
    }

    public int getAndAddCacheSize(int delta) {
        int prev = cacheSize.get();
        this.cacheSize.getAndAdd(delta);
        this.heapSize.getAndAdd(delta);
        return prev;
    }

    public int getHeapSize() {
        return heapSize.get();
    }

    public int getDiskSize() {
        return diskSize.get();
    }

    public int getAndAddDiskSize(int delta) {
        int prev = diskSize.get();
        this.diskSize.getAndAdd(delta);
        this.heapSize.getAndAdd(-1 * delta);
        return prev;
    }

    public int getDiskFiles() {
        return diskFiles.get();
    }

    public long getPersistedFiles() {
        return persistedFiles.get();
    }

    public long getAndAddPersistedFiles(int delta) {
        long prev = persistedFiles.get();
        this.persistedFiles.getAndAdd(delta);
        this.diskFiles.getAndAdd(delta);
        return prev;
    }

    public long getLoadedFiles() {
        return loadedFiles.get();
    }

    public long getAndAddLoadedFiles(int delta) {
        long prev = loadedFiles.get();
        this.loadedFiles.getAndAdd(delta);
        this.diskFiles.getAndAdd((int) (-1 * delta));
        return prev;
    }

    public long getDiskFileSize() {
        return diskFileSize.get();
    }

    public long getAndAddDiskFileSize(long delta) {
        long prev = diskFileSize.get();
        this.diskFileSize.getAndAdd(delta);
        return prev;
    }

    public long getPerObjectDiskSize(){
        if(diskSize.get()<1){
            return 0;
        }
        else {
            return diskFileSize.get() / diskSize.get();
        }
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();

        sb.append("size="+cacheSize.get());
        sb.append(", heap="+ heapSize.get());
        sb.append(", disk="+ diskSize.get());
        sb.append(", files="+diskFiles.get());
        sb.append(", persisted="+persistedFiles.get());
        sb.append(", diskLoaded="+ loadedFiles.get());
        sb.append(", diskUsed="+getFileSizeStr(diskFileSize.get()));
        sb.append(", PerObjectSize="+getFileSizeStr(getPerObjectDiskSize()));

        return sb.toString();
    }

    protected String getFileSizeStr(long length){
        DecimalFormat df = new DecimalFormat("#.00");
        String fileSizeString = "";
        if (length < 1024) {
            fileSizeString = df.format((double) length) + "B";
        } else if (length < 1048576) {
            fileSizeString = df.format((double) length / 1024) + "K";
        } else if (length < 1073741824) {
            fileSizeString = df.format((double) length / 1048576) + "M";
        } else {
            fileSizeString = df.format((double) length / 1073741824) +"G";
        }
        return fileSizeString;
    }
}
