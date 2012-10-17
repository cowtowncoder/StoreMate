package com.fasterxml.storemate.client.call;

import java.io.*;

/**
 * Simple {@link GetContentProcessor} implementation for GETting content
 * and storing it in {@link java.io.File} specified.
 */
public class GetContentProcessorForFiles extends GetContentProcessor<File>
{
    protected final File _file;

    public GetContentProcessorForFiles(File f) { _file = f; }
    @Override public GetContentProcessorForFiles.Handler createHandler() {
        return new Handler(_file);
    }

    /**
     * Simple {@link PutContentProvider} implementation that is backed by
     * specific File. More advanced implementations would probably try creating
     * new temporary files instead.
     */
    public static class Handler extends GetContentProcessor.Handler<File>
    {
        protected final File _file;

        protected FileOutputStream _out;
        
        public Handler(File f) {
            if (f == null) throw new IllegalArgumentException("Can not pass null File");
            _file = f;
        }

        @Override
        public void processContent(byte[] content, int offset, int length)
            throws IOException
        {
            if (_out == null) {
                _out = new FileOutputStream(_file);
            }
            if (length > 0) {
                _out.write(content, offset, length);
            }
        }

        @Override
        public File completeContentProcessing() throws IOException
        {
            /* one tricky thing: empty files have no content, and so {@link #processContent}
             * will not be called. But we still want the (empty) File...
             */
            if (_out == null) {
                _out = new FileOutputStream(_file);
            }
            _out.close();
            return _file;
        }

        @Override
        public void contentProcessingFailed(Throwable cause)
        {
            if (_out != null) {
                try {
                    _out.close();
                } catch (IOException e) { }            
                _file.delete();
            }
        }
    }
}