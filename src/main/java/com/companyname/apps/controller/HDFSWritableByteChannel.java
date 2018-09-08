package com.companyname.apps.controller;

/* This code snippet is a part of the blog at
https://github.com/animeshtrivedi/blog/blob/master/post/2017-12-26-arrow.md
*/

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Created by atr on 19.12.17.
 */
public class HDFSWritableByteChannel implements WritableByteChannel {

    private FSDataOutputStream outStream;
    private Boolean isOpen;
    private byte[] tempBuffer;

    public HDFSWritableByteChannel(FSDataOutputStream outStream){
        this.outStream = outStream;
        this.isOpen = true;
        // 1MB buffering
        this.tempBuffer = new byte[1024*1024];
    }

    private int writeDirectBuffer(ByteBuffer src) throws IOException {
        int remaining = src.remaining();
        int soFar = 0;
        while(soFar < remaining){
            int toPush = Math.min(remaining - soFar, this.tempBuffer.length);
            // this will move the position index
            src.get(this.tempBuffer, 0, toPush);
            // i have no way of knowing how much can i push at HDFS
            this.outStream.write(this.tempBuffer, 0, toPush);
            soFar+=toPush;
        }
        return remaining;
    }

    private int writeHeapBuffer(ByteBuffer src) throws IOException {
        int remaining = src.remaining();
        // get the heap buffer directly and copy
        this.outStream.write(src.array(), src.position(), remaining);
        src.position(src.position() + remaining);
        return remaining;
    }

    @Override
    final public int write(ByteBuffer src) throws IOException {
        if(src.isDirect()){
            return writeDirectBuffer(src);
        } else {
            return writeHeapBuffer(src);
        }
    }

    @Override
    final public boolean isOpen() {
        return this.isOpen;
    }

    @Override
    final public void close() throws IOException {
        // flushes the client buffer
        this.outStream.hflush();
        // to the disk
        this.outStream.hsync();
        this.outStream.close();
        this.isOpen = false;
    }
}
