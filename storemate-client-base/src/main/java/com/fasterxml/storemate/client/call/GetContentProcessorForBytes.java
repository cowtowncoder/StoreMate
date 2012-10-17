package com.fasterxml.storemate.client.call;

import java.io.IOException;

import com.fasterxml.storemate.shared.util.ByteAggregator;

/**
 * Simple {@link GetContentProcessor} implementation for GETting content
 * and aggregating it in (and returning as) {@link ByteAggregator}.
 */
public class GetContentProcessorForBytes extends GetContentProcessor<ByteAggregator>
{
    @Override public GetContentProcessorForBytes.Handler createHandler() {
        return new Handler();
    }

    /**
     * Simple {@link PutContentProvider} implementation that is backed by
     * specific File. More advanced implementations would probably try creating
     * new temporary files instead.
     */
    public static class Handler extends GetContentProcessor.Handler<ByteAggregator>
    {
        protected  ByteAggregator _bytes;
        
        public Handler() { }

        @Override
        public void processContent(byte[] content, int offset, int length)
            throws IOException
        {
            _bytes = ByteAggregator.with(_bytes, content, offset, length);
        }

        @Override
        public ByteAggregator completeContentProcessing() throws IOException
        {
            if (_bytes == null) {
                _bytes = new ByteAggregator();
            }
            return _bytes;
        }

        @Override
        public void contentProcessingFailed(Throwable cause) { }
    }
}