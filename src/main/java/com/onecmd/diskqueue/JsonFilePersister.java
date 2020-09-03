package com.onecmd.diskqueue;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class JsonFilePersister<T> implements CachePersiter{

    private static Logger LOGGER = LoggerFactory.getLogger(JsonFilePersister.class);

    private ObjectMapper jsonMapper = new ObjectMapper();
    private static final int FAILED_RETRY_TIMES = 3;

    public JsonFilePersister(){
        jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public long write(File file, List objectList) throws Exception{
        return retryWriteDataToFile(file, objectList);
    }

    @Override
    public List<T> read(File file, Class objectType) throws Exception{
        JavaType javaType = jsonMapper.getTypeFactory().constructParametricType(ArrayList.class, objectType);
        return retryReadDataToFile(file, javaType);
    }

    private long retryWriteDataToFile(File file, List<T> buffer) throws Exception {
        int failedTimes = FAILED_RETRY_TIMES;
        IOException exception= null;

        while ((failedTimes--) > 0) {
            try {
                long size = writeDataToFile(file, buffer);
                return size;
            } catch (IOException e) {
                exception = e;
                LOGGER.trace("Try to WriteDataToFile failed(failedTimes=" + failedTimes + "): " + e.getMessage(), e);
            }
        }

        FileUtils.deleteQuietly(file);
        throw exception;
    }

    private long writeDataToFile(File file, List<T> buffer) throws Exception {
        FileUtils.deleteQuietly(file);
        if(!file.exists()){
            file.createNewFile();
        }
        else {
            throw new IOException("File exist: "+file.getName() +", failed to delete it.");
        }
        jsonMapper.writeValue(file, buffer);
        return file.length();
    }

    private ArrayList<T> retryReadDataToFile(File file, JavaType jacksonJavaType) throws Exception {
        int failedTimes = FAILED_RETRY_TIMES;
        IOException exception= null;
        while ((failedTimes--) > 0)  {
            try {
                ArrayList<T> diskCaches = jsonMapper.readValue(file, jacksonJavaType);
                return diskCaches;
            }
            catch (IOException e){
                exception = e;
                LOGGER.trace("Try to WriteDataToFile failed(failedTimes="+failedTimes+"): " + e.getMessage(), e);
            }
        }

        throw exception;
    }

}
