package com.onecmd.diskqueue;

import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 */
public class CacheSubPoolTest {

    private String getPath(){
        File file = new File(this.getClass().getResource(".").getPath()+File.separator+"test");
        FileUtils.deleteQuietly(file);
        file.mkdirs();

        System.out.println("Path: "+file.getAbsolutePath());

        return file.getAbsolutePath();
    }

    private CacheConfig createConfig(){
        CacheConfig config = new CacheConfig();
        config.setCapacity(100);
        config.setDiskCacheFileRoot(getPath());
        config.setUsingDisk(true);
        config.setHeapCapacity(20);
        config.setPageSize(5);

        return config;
    }

    @Test
    public void testPersistEmpty_Expect_NoFilePersisted() throws Exception {
        CacheConfig config = createConfig();
        CacheStatistics statistics = new CacheStatistics();
        CachePersiter<Integer> persiter = new JsonFilePersister<Integer>();

        int poolId = 3;
        CacheSubPool<Integer> subPool = new CacheSubPool<>(poolId, config, statistics,  Integer.class);
        subPool.setCachePersiter(persiter);

        assertEquals(poolId, subPool.getId());

        String filePath = config.getDiskCacheFileRoot()+File.separator+poolId+".dat";
        assertEquals("file path not right", filePath, subPool.getFilePath());

        int persist = subPool.persist();
        assertEquals(0, persist);

        assertTrue(!new File(filePath).exists());
    }

    @Test
    public void testPersistNotEmpty_Expect_FilePersisted() throws Exception {
        CacheConfig config = createConfig();
        CacheStatistics statistics = new CacheStatistics();
        CachePersiter<Integer> persiter = new JsonFilePersister<Integer>();

        int poolId = 3;
        CacheSubPool<Integer> subPool = new CacheSubPool<>(poolId, config, statistics,  Integer.class);
        subPool.setCachePersiter(persiter);

        String filePath = config.getDiskCacheFileRoot()+File.separator+poolId+".dat";

        int total = 30;
        for(int i=0; i< total; i++){
            subPool.add(i);
        }

        assertEquals("size not right", total, subPool.size());
        assertEquals("heap size not right", total, subPool.getHeapSize());
        assertEquals("disk size should be 0 before persist", 0, subPool.getSizeInDisk());
//        assertEquals(false, subPool.isInDisk());

        int persist = subPool.persist();
        assertEquals(total, persist);

        assertEquals("size not right", total, subPool.size());
        assertEquals("heap size should be 0 after persist", 0, subPool.getHeapSize());
        assertEquals("disk size not right", total, subPool.getSizeInDisk());
//        assertEquals(true, subPool.isInDisk());

        assertTrue(new File(filePath).exists());
        subPool.clear();
    }

    @Test
    public void testDraigToWhenNotPersist_Expect_Draged() throws Exception {
        CacheConfig config = createConfig();
        CacheStatistics statistics = new CacheStatistics();
        CachePersiter<Integer> persiter = new JsonFilePersister<Integer>();

        int poolId = 3;
        CacheSubPool<Integer> subPool = new CacheSubPool<>(poolId, config, statistics,  Integer.class);
        subPool.setCachePersiter(persiter);

        String filePath = config.getDiskCacheFileRoot()+File.separator+poolId+".dat";

        int total = 30;
        for(int i=0; i< total; i++){
            subPool.add(i);
        }

        ArrayList<Integer> list = new ArrayList<>();
        subPool.drainTo(list, total);
        assertEquals(total, list.size());
        for(int i=0; i< total; i++){
            assertTrue("Draig out data not FIFO", list.get(i).equals(i));
        }
        assertEquals("size not right", 0, subPool.size());
        assertEquals("heap size not right", 0, subPool.getHeapSize());
        assertEquals("disk size not right", 0, subPool.getSizeInDisk());

        subPool.clear();
    }

    @Test
    public void testDraigToWhenAfterPersist_Expect_Draged() throws Exception {
        CacheConfig config = createConfig();
        CacheStatistics statistics = new CacheStatistics();
        CachePersiter<Integer> persiter = new JsonFilePersister<Integer>();

        int poolId = 3;
        CacheSubPool<Integer> subPool = new CacheSubPool<>(poolId, config, statistics,  Integer.class);
        subPool.setCachePersiter(persiter);

        String filePath = config.getDiskCacheFileRoot()+File.separator+poolId+".dat";

        int total = 30;
        for(int i=0; i< total; i++){
            subPool.add(i);
        }

        subPool.persist();

        ArrayList<Integer> list = new ArrayList<>();
        subPool.drainTo(list, total);
        assertEquals(total, list.size());
        for(int i=0; i< total; i++){
            assertTrue("Draig out data not FIFO", list.get(i).equals(i));
        }
        assertEquals("size not right", 0, subPool.size());
        assertEquals("heap size not right", 0, subPool.getHeapSize());
        assertEquals("disk size not right", 0, subPool.getSizeInDisk());
        assertTrue(!new File(filePath).exists());

        subPool.clear();
    }

    @Test
    public void testClear_Expect_FileDeletedAndPoolEmpty() throws Exception {
        CacheConfig config = createConfig();
        CacheStatistics statistics = new CacheStatistics();
        CachePersiter<Integer> persiter = new JsonFilePersister<Integer>();

        int poolId = 3;
        CacheSubPool<Integer> subPool = new CacheSubPool<>(poolId, config, statistics,  Integer.class);
        subPool.setCachePersiter(persiter);

        String filePath = config.getDiskCacheFileRoot()+File.separator+poolId+".dat";

        int total = 30;
        for(int i=0; i< total; i++){
            subPool.add(i);
        }

        subPool.persist();
        assertTrue(new File(filePath).exists());
        TestCase.assertEquals(total, subPool.size());

        subPool.clear();
        assertTrue(!new File(filePath).exists());
        TestCase.assertEquals(0, subPool.size());
    }

    @Test
    public void testStatictisWhenNoData_Expect_NoChanged() throws Exception {
        CacheConfig config = createConfig();
        CacheStatistics statistics = new CacheStatistics();
        CachePersiter<Integer> persiter = new JsonFilePersister<Integer>();

        int poolId = 3;
        CacheSubPool<Integer> subPool = new CacheSubPool<>(poolId, config, statistics,  Integer.class);
        subPool.setCachePersiter(persiter);

        TestCase.assertEquals(0, statistics.getCacheSize());
        TestCase.assertEquals(0, statistics.getHeapSize());
        TestCase.assertEquals(0, statistics.getDiskSize());
        TestCase.assertEquals(0, statistics.getDiskFiles());
        TestCase.assertEquals(0, statistics.getPersistedFiles());
        TestCase.assertEquals(0, statistics.getLoadedFiles());

        subPool.persist();

        TestCase.assertEquals(0, statistics.getCacheSize());
        TestCase.assertEquals(0, statistics.getHeapSize());
        TestCase.assertEquals(0, statistics.getDiskSize());
        TestCase.assertEquals(0, statistics.getDiskFiles());
        TestCase.assertEquals(0, statistics.getPersistedFiles());
        TestCase.assertEquals(0, statistics.getLoadedFiles());

    }

    @Test
    public void testStatictis_Expect_Ok() throws Exception {
        CacheConfig config = createConfig();
        CacheStatistics statistics = new CacheStatistics();
        CachePersiter<Integer> persiter = new JsonFilePersister<Integer>();

        int poolId = 3;
        CacheSubPool<Integer> subPool = new CacheSubPool<>(poolId, config, statistics,  Integer.class);
        subPool.setCachePersiter(persiter);

        int total = 30;
        for(int i=0; i< total; i++){
            statistics.getAndAddCacheSize(1);
            subPool.add(i);
        }

        TestCase.assertEquals(total, statistics.getCacheSize());
        TestCase.assertEquals(total, statistics.getHeapSize());
        TestCase.assertEquals(0, statistics.getDiskSize());
        TestCase.assertEquals(0, statistics.getDiskFiles());
        TestCase.assertEquals(0, statistics.getPersistedFiles());
        TestCase.assertEquals(0, statistics.getLoadedFiles());
        TestCase.assertTrue(statistics.getDiskFileSize() == 0);
        TestCase.assertTrue(statistics.getPerObjectDiskSize() == 0);

        System.out.println(statistics.toString());

        subPool.persist();

        TestCase.assertEquals(total, statistics.getCacheSize());
        TestCase.assertEquals(0, statistics.getHeapSize());
        TestCase.assertEquals(total, statistics.getDiskSize());
        TestCase.assertEquals(1, statistics.getDiskFiles());
        TestCase.assertEquals(1, statistics.getPersistedFiles());
        TestCase.assertEquals(0, statistics.getLoadedFiles());
        TestCase.assertTrue(statistics.getDiskFileSize() > 0);
        TestCase.assertTrue(statistics.getPerObjectDiskSize() >0);

        System.out.println(statistics.toString());

        ArrayList<Integer> list = new ArrayList<>();
        subPool.drainTo(list, total);
        statistics.getAndAddCacheSize(-1 * list.size());

        TestCase.assertEquals(0, statistics.getCacheSize());
        TestCase.assertEquals(0, statistics.getHeapSize());
        TestCase.assertEquals(0, statistics.getDiskSize());
        TestCase.assertEquals(0, statistics.getDiskFiles());
        TestCase.assertEquals(1, statistics.getPersistedFiles());
        TestCase.assertEquals(1, statistics.getLoadedFiles());
        TestCase.assertTrue(statistics.getDiskFileSize() == 0);
        TestCase.assertTrue(statistics.getPerObjectDiskSize() == 0);

        System.out.println(statistics.toString());
    }

    @Test
    public void testConcurrency_Expect_Ok(){
        CacheConfig config = createConfig();
        CacheStatistics statistics = new CacheStatistics();
        CachePersiter<Integer> persiter = new JsonFilePersister<Integer>();

        int poolId = 3;
        CacheSubPool<Integer> subPool = new CacheSubPool<>(poolId, config, statistics,  Integer.class);
        subPool.setCachePersiter(persiter);
//
//        assertTrue(!subPool.isInDisk());
//        assertTrue(!subPool.getAndSetInDisk(true));
//        assertTrue(subPool.isInDisk());
//        assertTrue(subPool.getAndSetInDisk(false));
//        assertTrue(!subPool.isInDisk());
    }

    @Test
    public void testWriteFailedWhenPersist_Expect_ReturnZero() throws Exception {
        CacheConfig config = createConfig();
        CacheStatistics statistics = new CacheStatistics();
        CachePersiter<Integer> persiter = mock(CachePersiter.class);

        when(persiter.write(any(File.class), any(List.class)))
                .thenThrow(new IOException("Disk full"));

        int poolId = 3;
        CacheSubPool<Integer> subPool = new CacheSubPool<>(poolId, config, statistics,  Integer.class);
        subPool.setCachePersiter(persiter);

        int total = 30;
        for(int i=0; i< total; i++){
            statistics.getAndAddCacheSize(1);
            subPool.add(i);
        }

        try {
            int persisted = subPool.persist();
            assertEquals(0, persisted);
        }
        catch (IOException e){
            fail("Expect no exception and just return 0.");
        }

        TestCase.assertEquals(total, subPool.size());
        ArrayList list = new ArrayList();
        subPool.drainTo(list, total);
        for(int i=0; i< total; i++){
            assertTrue("Draig out data not FIFO after write failed", list.get(i).equals(i));
        }

    }

    @Test
    public void testReadFailedWhenDrag_Expect_SizeNoChange() throws Exception {
        CacheConfig config = createConfig();
        CacheStatistics statistics = new CacheStatistics();
        CachePersiter<Integer> persiter = new CachePersiter(){

            @Override
            public long write(File file, List objectList) throws Exception {
                return new JsonFilePersister().write(file, objectList);
            }

            @Override
            public List read(File file, Class objectType) throws IOException {
                throw new IOException("Disk full");
            }
        };

        int poolId = 3;
        CacheSubPool<Integer> subPool = new CacheSubPool<>(poolId, config, statistics,  Integer.class);
        subPool.setCachePersiter(persiter);

        int total = 30;
        for(int i=0; i< total; i++){
            statistics.getAndAddCacheSize(1);
            subPool.add(i);
        }

        subPool.persist();
        ArrayList<Integer> list = new ArrayList<>();

        try {
            subPool.drainTo(list, total);
            fail("Expect no exception and just return 0.");
        }
        catch (IOException e){

        }

        TestCase.assertEquals(total, subPool.size());
    }

    @Test
    public void testAddListToQueueHead_Expect_OK(){
        CacheConfig config = createConfig();
        CacheStatistics statistics = new CacheStatistics();

        int poolId = 3;
        CacheSubPool<Integer> subPool = new CacheSubPool<>(poolId, config, statistics,  Integer.class);

        LinkedBlockingDeque<Integer> queue = new LinkedBlockingDeque<>();

        int maxValue = 50;
        queue.add(maxValue);

        ArrayList<Integer> list = new ArrayList<>();

        subPool.addListToQueueHead(queue, list);

        TestCase.assertEquals(maxValue, queue.getFirst().intValue());

        for(int i=0; i< maxValue; i++){
            list.add(i);
        }

        subPool.addListToQueueHead(queue, list);
        for(int i=0; i< maxValue +1; i++){
            TestCase.assertEquals(i, queue.poll().intValue());
        }

    }
}
