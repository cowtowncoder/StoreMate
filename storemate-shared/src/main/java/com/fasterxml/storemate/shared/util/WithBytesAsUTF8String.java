package com.fasterxml.storemate.shared.util;


/**
 * Simple {@link WithBytesCallback} implementation to use when
 * when you need contents to be converted to a UTF-8 String.
 */
public class WithBytesAsUTF8String implements WithBytesCallback<String>
{
   public final static WithBytesAsUTF8String instance = new WithBytesAsUTF8String();

   @Override
   public String withBytes(byte[] buffer, int offset, int length) 
   {
       if (length <= 0) {
           return "";
       }
       return UTF8Encoder.decodeFromUTF8(buffer, offset, length);
   }
}
