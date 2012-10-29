package com.fasterxml.storemate.shared.util;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.RequestPathBuilder;

/**
 * Simple {@link RequestPathBuilder} implementation that can be used
 * with the default JDK http client.
 */
public class JdkHttpClientPathBuilder extends RequestPathBuilder
{
    protected final static UTF8UrlEncoder _urlEncoder = new UTF8UrlEncoder();
    
    protected final String _serverPart;

    protected String _path;

    protected List<String> _queryParams;
     
    public JdkHttpClientPathBuilder(IpAndPort server) {
        this(server, null, null);
    }

    public JdkHttpClientPathBuilder(String serverPart) {
        this(serverPart, null, (String[]) null);
    }

    public JdkHttpClientPathBuilder(String serverPart, String path) {
        this(serverPart, path, (String[]) null);
    }
    
    public JdkHttpClientPathBuilder(IpAndPort server, String path, String[] qp) {
        this(server.getEndpoint(), path, _arrayToList(qp));
    }

    public JdkHttpClientPathBuilder(String serverPart, String path, String[] qp) {
        this(serverPart, path, _arrayToList(qp));
    }

    public JdkHttpClientPathBuilder(String serverPart, String path, List<String> qp)
    {
        _serverPart = serverPart;
        _path = path;
        _queryParams = qp;
    }

    /*
    /*********************************************************************
    /* API impl
    /*********************************************************************
     */
     
    @Override
    public RequestPathBuilder addPathSegment(String segment)
    {
          if (_path == null) {
               _path = _urlEncoder.encode(segment);
          } else {
               StringBuilder sb = new StringBuilder(_path);
               sb.append('/');
               if (segment != null && segment.length() > 0) {
                    sb = _urlEncoder.appendEncoded(sb, segment);
               }
               _path = sb.toString();
          }
          return this;
    }

    @Override
    public RequestPathBuilder addParameter(String key, String value)
    {
         if (_queryParams == null) {
              _queryParams = new ArrayList<String>(8);
         }
         _queryParams.add(key);
         _queryParams.add(value);
         return this;
    }

    @Override
    public String getServerPart() {
        return _serverPart;
    }
    
    @Override
    public String getPath() {
        return _path;
    }
    
    @Override
    public JdkHttpClientPath build() {
        return new JdkHttpClientPath(_serverPart, _path, _queryParams);
    }

    protected String _url()
    {
         if (_path == null) {
              return _serverPart;
         }
         if (_queryParams == null) {
             return _serverPart + _path;
         }
         StringBuilder sb = new StringBuilder(100);
         sb.append(_serverPart);
         sb.append(_path);
         final int len = _queryParams.size();
         if (len > 0) {
             sb.append('?');
             for (int i = 0; i < len; i += 2) {
                 sb.append(_queryParams.get(i)).append('=');
                 _urlEncoder.appendEncoded(sb, _queryParams.get(i+1));
             }
         }
         return sb.toString();
    }

    @Override
    public String toString() {
         return _url();
    }
}
