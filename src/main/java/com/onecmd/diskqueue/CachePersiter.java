package com.onecmd.diskqueue;

import java.io.File;
import java.util.List;

/**
 */
public interface CachePersiter<T> {

    long write(File file, List<T> objectList) throws Exception;

    List<T> read(File file, Class<T> objectType) throws Exception;
}
