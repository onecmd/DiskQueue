package com.onecmd.diskqueue;

import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

/**
 */
public class CacheStatisticsTest {

    private CacheStatistics statistics = null;

    @Before
    public void setup(){
        statistics = new CacheStatistics();
    }

    @Test
    public void testCacheSize(){
        int rnd = new Random().nextInt(100);
        int prev = statistics.getAndAddCacheSize(rnd);
        assertEquals(prev, 0);
        assertEquals(prev+rnd,statistics.getCacheSize());
        assertEquals(prev+rnd,statistics.getHeapSize());
    }

    @Test
    public void testDiskSize(){
        int rnd1 = new Random().nextInt(100);
        int delta = 10;
        statistics.getAndAddCacheSize(rnd1+delta);
        int prev = statistics.getAndAddDiskSize(delta);

        assertEquals(prev, 0);
        assertEquals(rnd1+delta,statistics.getCacheSize());
        assertEquals(rnd1,statistics.getHeapSize());
        assertEquals(delta,statistics.getDiskSize());
    }

    @Test
    public void testPersistedFiles(){
        int rnd = new Random().nextInt(100);
        long prev = statistics.getAndAddPersistedFiles(rnd);
        assertEquals(prev, 0);
        assertEquals(prev+rnd,statistics.getDiskFiles());
        assertEquals(prev+rnd,statistics.getPersistedFiles());
    }

    @Test
    public void testLoadedFromDisk(){
        int rnd = new Random().nextInt(100);
        int delta = 10;
        statistics.getAndAddPersistedFiles(rnd+delta);

        long prev = statistics.getAndAddLoadedFiles(delta);
        assertEquals(prev, 0);
        assertEquals(prev+rnd,statistics.getDiskFiles());
        assertEquals(delta,statistics.getLoadedFiles());
    }


    @Test
    public void testGetFileSizeStr(){

        assertTrue(statistics.getFileSizeStr(1023).endsWith("B"));
        assertTrue(statistics.getFileSizeStr(1048575).endsWith("K"));
        assertTrue(statistics.getFileSizeStr(1073741823).endsWith("M"));
        assertTrue(statistics.getFileSizeStr(1073741824).endsWith("G"));
    }

}
