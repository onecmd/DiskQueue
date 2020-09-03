package com.onecmd.diskqueue;

import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static junit.framework.Assert.fail;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

/**
 */
public class DiskQueueTest {

    @Test
    public void testCreation_Expect_Success(){
        String name = "test";
        String path = "./testqueue";
        DiskQueue<Integer> queue = new DiskQueue(name, Integer.class, 10000, 3000, 0, 1000, true, 10, path );
        queue.getConfig().setPersistTimeoutSeconds(15);

        assertEquals(0, queue.size());
        assertEquals(10000, queue.getConfig().getCapacity());
        assertEquals(3000, queue.getConfig().getHeapCapacity());
        assertEquals(1000, queue.getConfig().getPageSize());
        assertEquals(true, queue.getConfig().isUsingDisk());
        assertTrue(new File(path).exists());
        assertEquals(15, queue.getConfig().getPersistTimeoutSeconds());

        TestCase.assertNotNull(queue.getStatistics());
        TestCase.assertNotNull(queue.getSummary());
    }

    @Test
    public void testNormalSingleType_Expect_Success() throws Exception {

        String name = "test";
        DiskQueue<Integer> queue = new DiskQueue(name, Integer.class, 100, 20, 0, 10, true, 10, "./testqueue" );

        assertEquals(0, queue.size());
        assertTrue(queue.isEmpty());

        int total = 30;
        for(int i=0; i<total; i++) {
            queue.add(i);
        }
        assertEquals(total, queue.size());
        assertTrue(!queue.isEmpty());

        int k = 0;
        ArrayList<Integer> list = new ArrayList<>();
        queue.drainTo(list, 10);
        System.out.println(list);
        for(int i=0; i< list.size(); i++){
            assertEquals("Not FIFO", k + i, list.get(i).intValue());
        }
        k+=list.size();

        list = new ArrayList<>();
        queue.drainTo(list, 10);
        System.out.println(list);
        for(int i=0; i< list.size(); i++){
            assertEquals("Not FIFO", k + i, list.get(i).intValue());
        }
        k+=list.size();

        list = new ArrayList<>();
        queue.drainTo(list, 10);
        System.out.println(list);
        for(int i=0; i< list.size(); i++){
            assertEquals("Not FIFO", k + i, list.get(i).intValue());
        }

        queue.clear();
        assertTrue(queue.isEmpty());
    }

    private CacheExample createExample(int id){
        CacheExample example = new CacheExample();
        example.setId(id);
        example.setName("name_" + id);
        example.setTime(new Date());
        example.setAddtionalText("add_"+id);
        return example;
    }

    private String str_100B = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
    private String str_1k = str_100B+str_100B+str_100B+str_100B+str_100B+str_100B+str_100B+str_100B+str_100B+str_100B;

    private CacheExample create1KSizeExample(int id){
        CacheExample example = new CacheExample();
        example.setId(id);
        example.setName("name_" + id);
        example.setTime(new Date());
        example.setAddtionalText("add_" + id + "_" + str_1k);
        return example;
    }

    @Test
    public void testNormalObject_Expect_Success() throws Exception {
        String name = "test";
        DiskQueue<CacheExample> queue = new DiskQueue(name, CacheExample.class, 100, 20,0,  10, true,10,  "./testqueue" );

        queue.clear();

        ArrayList<CacheExample> before = new ArrayList<>();
        int total = 30;
        for(int i=0; i<total; i++) {
            CacheExample example = createExample(i);
            before.add(example);
            queue.add(example);
        }
        assertEquals(total, queue.size());

        ArrayList<CacheExample> list = new ArrayList<>();
        queue.drainTo(list, before.size());

        for(int i=0; i< before.size(); i++){
            assertTrue("Not FIFO ", before.get(i).equals(list.get(i)));
        }

        queue.clear();
    }

    @Test
    public void testFolderRemoved_Expect_PersistFailed(){
        String name = "test";
        DiskQueue<CacheExample> queue = new DiskQueue(name, CacheExample.class, 100, 20, 100, 10, true, 10, "./testqueue" );

        queue.clear();

        FileUtils.deleteQuietly(new File(queue.getConfig().getDiskCacheFileRoot()));

        boolean failed = false;
        int total = 30;
            for(int i=0; i<total; i++) {
                CacheExample example = createExample(i);
                if(!queue.add(example));{
                    failed = true;
                    break;
                }
            }

        if(!failed) {
            fail("Expect throw exception");
        }
    }

    @Test
    public void testDataFileDamaged_Expect_LoadFailed() throws Exception {
        String name = "test";
        DiskQueue<CacheExample> queue = new DiskQueue(name, CacheExample.class, 100, 20,100,  10, true, 10, "./testqueue" );

        queue.clear();

        boolean failed = false;
        int total = 30;
        for(int i=0; i<total; i++) {
            CacheExample example = createExample(i);
            queue.add(example);
        }

        FileUtils.deleteQuietly(new File(queue.getConfig().getDiskCacheFileRoot()));

        ArrayList list = new ArrayList();
        try {
            int result = queue.drainTo(list, total);
            fail("Expect failed.");
        }
        catch (Exception e){

        }
        System.out.println(list);
        assertTrue(list.size() < total);
//        assertEquals(-1, result);

        queue.clear();
    }

    @Test
    public void testCapacityExceed_Expect_AddedFailed(){
        String name = "test";
        int total = 100;
        DiskQueue<Integer> queue = new DiskQueue(name, Integer.class, total, 20,100,  10, true, 10, "./testqueue" );

        for(int i=0; i<total; i++) {
            boolean result = queue.add(i);
            assertEquals(true, result);
        }

        boolean result = queue.add(total + 1);
        assertEquals(total, queue.size());
        assertEquals("Expect failed when capacity exceed.", false, result);

        queue.clear();
    }

    @Test
    public void testDiskFileSizeExceed_Expect_AddedFailed(){
        String name = "test";
        int total = 1025; // 1014 * 1k = 1M
        int maxDiskSizeInMB = 1;
        DiskQueue<CacheExample> queue = new DiskQueue(name, CacheExample.class, total, 20, maxDiskSizeInMB,  100, true,10,  "./testqueue" );

        boolean hasFailed = false;
        for(int i=0; i<total; i++) {
            boolean result = queue.offer(create1KSizeExample(i));
            if(result == false){
                hasFailed = true;
                break;
            }
        }

        assertEquals("Expect failed when persisted disk file size exceed.", true, hasFailed);

        queue.clear();
    }

    @Test
    public void testClear_ExpectNoFilesAndStatistisAreZero() {
        String name = "test";
        int total = 100;
        DiskQueue<Integer> queue = new DiskQueue(name, Integer.class, total, 20, 100, 10, true, 10, "./testqueue");

        for (int i = 0; i < total; i++) {
            boolean result = queue.add(i);
            assertEquals(true, result);
        }

        assertTrue("Persist files should max than 0", new File(queue.getConfig().getDiskCacheFileRoot()).list().length > 0);
        assertEquals(total, queue.size());

        queue.clear();

        assertTrue("Persist files should be 0", new File(queue.getConfig().getDiskCacheFileRoot()).list().length == 0);
        assertEquals(0, queue.size());
        assertEquals(0, queue.getStatistics().getCacheSize());
        assertEquals(0, queue.getStatistics().getHeapSize());
        assertEquals(0, queue.getStatistics().getDiskSize());
        assertEquals(0, queue.getStatistics().getDiskFileSize());
    }

    @Test
    public void testWrongParameters_Expect_ThrowExceptions(){
        String name = "test";
        int total = 100;
        DiskQueue<Integer> queue = new DiskQueue(name, Integer.class, total, 20, 100, 10, true,10,  "./testqueue");

        try {
            queue.add(null);
            fail("Expect throw NullPointerException when obj is null");
        }
        catch (NullPointerException e){

        }
        catch (Exception e){
            fail("Expect throw NullPointerException when obj is null, but is: "+e.getMessage());
        }

        try {
            queue.drainTo(null, 100);
            fail("Expect throw NullPointerException when list is null");
        }
        catch (NullPointerException e){

        }
        catch (Exception e){
            fail("Expect throw NullPointerException when list is null, but is: "+e.getMessage());
        }

    }

    @Test
    public void testConcurrent_Expect_NoDataLost() throws InterruptedException {
        String name = "test";
        int total = 100000;
        DiskQueue<Integer> queue = new DiskQueue(name, Integer.class, total, 1000, 200, 10, true,10,  "./testqueue");

        int end = 0;
        int start = 0;
        int eachSize = 400;

        ConcurrentHashMap<Integer, Integer> threadPool = new ConcurrentHashMap<>();

        for(int i=0; i<5; i++) {
            start = end + 1;
            end += eachSize;
            threadPool.put(start, start);
            new QueueAddThread(queue, start, end).start();
        }

        BlockingQueue<Integer> list = new LinkedBlockingQueue<>();
        new QueueGetThread(queue, list, 200).start();

        int getted = 0;
        long startTime = System.currentTimeMillis();
        while (true) {
            Thread.sleep(500);
            synchronized (list) {
                int speed =  list.size();

                Integer value = null;
                do{
                    value = list.poll();
                    if(null != value){
                        getted++;
                        assertTrue("Disk cache fetch sequence is not right.", checkValue(threadPool, value.intValue(), value-eachSize));
                    }
                }while (value != null);

                list.clear();
                System.out.println("====> end: "+end+", get: "+getted+", speed: " + speed);
            }
            if(getted >= end){
                break;
            }
            if(System.currentTimeMillis() - startTime > 15000){
                fail("Cost too much times, disk cache data may lost or speed is too low .");
            }
        }
    }

    private boolean checkValue(ConcurrentHashMap<Integer, Integer> threadPool, int value, int valueMin){
        Iterator<Map.Entry<Integer, Integer>> iter = threadPool.entrySet().iterator();
        while (iter.hasNext()){
            Map.Entry<Integer, Integer> entry = iter.next();
            if(entry.getKey().intValue() <= value && entry.getKey().intValue() > valueMin){
//                System.out.println(entry.getKey()+"."+entry.getValue()+" <==> "+value);
                if(value>=entry.getValue().intValue()){
                    entry.setValue(value);
                    return true;
                }
                else {
                    System.out.println(""+entry.getValue()+" <==> "+value);
                    return false;
                }
            }
        }
        return true;
    }

    class QueueAddThread extends Thread{
        DiskQueue<Integer> queue = null;
        int start = 1;
        int end = 100;

        public QueueAddThread(DiskQueue<Integer> queue, int start, int end){
            this.queue = queue;
            this.start = start;
            this.end = end;
        }

        public void run(){
            long startTime = System.currentTimeMillis();
            for(int i= start; i<=end; i++){
                queue.add(i);
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                }
            }
            long cost = System.currentTimeMillis() - startTime;
            System.out.println("Add["+start+", "+end+"] finished, cost: "+cost+" ms, speed: "+(end-start)*1000/cost+" /s.");
        }
    }
    class QueueGetThread extends Thread{
        DiskQueue<Integer> queue = null;
        BlockingQueue<Integer> list = null;
        int batchSize = 10;

        public QueueGetThread(DiskQueue<Integer> queue,BlockingQueue<Integer> list, int batchSize){
            this.queue = queue;
            this.list = list;
            this.batchSize = batchSize;
        }

        public void run(){
            while (true){
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
                try {
                    ArrayList<Integer> tmp = new ArrayList<>();
                    int total = queue.drainTo(tmp, batchSize);
                    list.addAll(tmp);
//                    System.out.println("Queue: "+queue.size()+", Get: "+total+", total: "+list.size());
                } catch (Exception e) {
                    System.out.println("Failed to get data: "+e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }
}
