package com.broadtech.analyse.task.test.reboot.util;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author leo.J
 * @description 关闭Java的IO
 * @date 2020-09-09 16:14
 */
public class FileTools {
    public static void close(Closeable... close) {

        for (Closeable io : close) {
            if (null != io) {
                try {
                    io.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
