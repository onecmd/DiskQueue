package com.onecmd.diskqueue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

/**
 */
public class JsonFilePersisterTest {

    private ObjectMapper mapper = new ObjectMapper();

    private String getPath(){
        File file = new File(this.getClass().getResource(".").getPath()+File.separator+"test");
        FileUtils.deleteQuietly(file);
        file.mkdirs();

        System.out.println("Path: "+file.getAbsolutePath());

        return file.getAbsolutePath();
    }

    @Test
    public void testReadWriteNormally_Expect_Success() throws Exception {

        ArrayList<Integer> list = new ArrayList<>();
        for(int i=0; i< 30; i++){
            list.add(i);
        }

        File file = new File(getPath()+File.separator+"text.dat");

        JsonFilePersister persister = new JsonFilePersister();
        persister.write(file, list);

        List<Integer> list2 = persister.read(file, Integer.class);

        for(int i=0; i<list.size(); i++){
            assertEquals(list.get(i), list2.get(i));
        }

    }

    @Test
    public void testReadFailed_Expect_ThrowException() throws IOException {

        File file = new File(getPath()+File.separator+"text.dat");
        FileUtils.write(file, "wrong value", Charset.defaultCharset());

        JsonFilePersister persister = new JsonFilePersister();

        try {
            persister.read(file, Integer.class);
            fail("Expect throw exception.");
        }
        catch (Exception e){

        }
    }


    @Test
    public void testWriteFailed_Expect_ThrowException() throws IOException {

        ArrayList<Integer> list = new ArrayList<>();
        for(int i=0; i< 30; i++){
            list.add(i);
        }

        File file = new File(getPath()+File.separator+"wrong");
        FileUtils.deleteQuietly(file);
        file = new File(getPath()+File.separator+"wrong/test.dat");

        JsonFilePersister persister = new JsonFilePersister();

        try {
            persister.write(file, list);
            fail("Expect throw exception.");
        }
        catch (Exception e){

        }
    }
}
