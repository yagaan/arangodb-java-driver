/*
 * DISCLAIMER
 *
 * Copyright 2017 ArangoDB GmbH, Cologne, Germany
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright holder is ArangoDB GmbH, Cologne, Germany
 */

package com.arangodb.internal.http;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.ProxyAuthenticationStrategy;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoDBException;
import com.arangodb.Protocol;
import com.arangodb.internal.net.Connection;
import com.arangodb.internal.net.HostDescription;
import com.arangodb.internal.net.ProxyDescription;
import com.arangodb.internal.util.CURLLogger;
import com.arangodb.internal.util.IOUtils;
import com.arangodb.internal.util.ResponseUtils;
import com.arangodb.util.ArangoSerialization;
import com.arangodb.util.ArangoSerializer.Options;
import com.arangodb.velocypack.VPackSlice;
import com.arangodb.velocystream.Request;
import com.arangodb.velocystream.Response;

/**
 * @author Mark Vollmary
 */
public class HttpConnection implements Connection {

	private static final Logger LOGGER = LoggerFactory.getLogger(HttpCommunication.class);
	private static final ContentType CONTENT_TYPE_APPLICATION_JSON_UTF8 = ContentType.create("application/json",
			"utf-8");
	private static final ContentType CONTENT_TYPE_VPACK = ContentType.create("application/x-velocypack");

	public static class Builder {
		private String user;
		private String password;
		private ArangoSerialization util;
		private Boolean useSsl;
		private String httpCookieSpec;
		private Protocol contentType;
		private HostDescription host;
		private String path = "";

		private Long ttl;
		private SSLContext sslContext;
		private Integer timeout;

		public Builder user(final String user) {
			this.user = user;
			return this;
		}

		public Builder password(final String password) {
			this.password = password;
			return this;
		}

		public Builder serializationUtil(final ArangoSerialization util) {
			this.util = util;
			return this;
		}

		public Builder useSsl(final Boolean useSsl) {
			this.useSsl = useSsl;
			return this;
		}

		public Builder httpCookieSpec(String httpCookieSpec) {
			this.httpCookieSpec = httpCookieSpec;
			return this;
		}

		public Builder contentType(final Protocol contentType) {
			this.contentType = contentType;
			return this;
		}

		public Builder host(final HostDescription host) {
			this.host = host;
			return this;
		}

		public Builder path(final String path) {
			this.path = path == null ? "" : path;
			return this;
		}

		public Builder ttl(final Long ttl) {
			this.ttl = ttl;
			return this;
		}

		public Builder sslContext(final SSLContext sslContext) {
			this.sslContext = sslContext;
			return this;
		}

		public Builder timeout(final Integer timeout) {
			this.timeout = timeout;
			return this;
		}

		public HttpConnection build() {
			return new HttpConnection(this.host, this.path, this.timeout, this.user, this.password, this.useSsl,
					this.sslContext, this.util, this.contentType, this.ttl, this.httpCookieSpec);
		}
	}

	private final PoolingHttpClientConnectionManager cm;
	private final CloseableHttpClient client;
	private final String user;
	private final String password;
	private final ArangoSerialization util;
	private final Boolean useSsl;
	private final Protocol contentType;
	private final String baseUrl;

	private HttpConnection(final HostDescription host, final String path, final Integer timeout, final String user,
			final String password, final Boolean useSsl, final SSLContext sslContext, final ArangoSerialization util,
			final Protocol contentType, final Long ttl, final String httpCookieSpec) {
		super();
		this.baseUrl = buildBaseUrl(host, path);
		this.user = user;
		this.password = password;
		this.useSsl = useSsl;
		this.util = util;
		this.contentType = contentType;
		final RegistryBuilder<ConnectionSocketFactory> registryBuilder = RegistryBuilder.create();
		registryBuilder.register("http", new PlainConnectionSocketFactory());
		if (Boolean.TRUE == useSsl) {
			if (sslContext != null) {
				registryBuilder.register("https", new SSLConnectionSocketFactory(sslContext));
			} else {
				registryBuilder.register("https", new SSLConnectionSocketFactory(SSLContexts.createSystemDefault()));
			}
		}
		this.cm = new PoolingHttpClientConnectionManager(registryBuilder.build());
		this.cm.setDefaultMaxPerRoute(1);
		this.cm.setMaxTotal(1);
		final RequestConfig.Builder requestConfig = RequestConfig.custom();
		if (timeout != null && timeout >= 0) {
			requestConfig.setConnectTimeout(timeout);
			requestConfig.setConnectionRequestTimeout(timeout);
			requestConfig.setSocketTimeout(timeout);
		}

		if (httpCookieSpec != null && httpCookieSpec.length() > 1) {
			requestConfig.setCookieSpec(httpCookieSpec);
		}

		final ConnectionKeepAliveStrategy keepAliveStrategy = (response, context) -> HttpConnection.this
				.getKeepAliveDuration(response);
		final HttpClientBuilder builder = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig.build())
				.setConnectionManager(this.cm).setKeepAliveStrategy(keepAliveStrategy)
				.setRetryHandler(new DefaultHttpRequestRetryHandler());
		final ProxyDescription proxy = host.getProxy();
		if (proxy != null) {
			final HttpHost httpProxy = new HttpHost(proxy.getHost(), proxy.getPort());
			builder.setProxy(httpProxy);
			if (proxy.hasAuth()) {
				final CredentialsProvider credsProvider = new BasicCredentialsProvider();
				final UsernamePasswordCredentials proxyCreds = new UsernamePasswordCredentials(proxy.getUser(),
						proxy.getPassword());
				credsProvider.setCredentials(new AuthScope(httpProxy), proxyCreds);
				final ProxyAuthenticationStrategy pauth = new ProxyAuthenticationStrategy();
				builder.setProxyAuthenticationStrategy(pauth);
			}
		}
		if (ttl != null) {
			builder.setConnectionTimeToLive(ttl, TimeUnit.MILLISECONDS);
		}
		this.client = builder.build();
	}

	private long getKeepAliveDuration(final HttpResponse response) {
		final HeaderElementIterator it = new BasicHeaderElementIterator(response.headerIterator(HTTP.CONN_KEEP_ALIVE));
		while (it.hasNext()) {
			final HeaderElement he = it.nextElement();
			final String param = he.getName();
			final String value = he.getValue();
			if (value != null && "timeout".equalsIgnoreCase(param)) {
				try {
					return Long.parseLong(value) * 1000L;
				} catch (final NumberFormatException ignore) {
				}
			}
		}
		return 30L * 1000L;
	}

	@Override
	public void close() throws IOException {
		this.cm.shutdown();
		this.client.close();
	}

	private static String buildUrl(final String baseUrl, final Request request) {
		final StringBuilder sb = new StringBuilder().append(baseUrl);
		/* removing the last '/' char, if any */
		if (sb.length() > 0 && sb.charAt(sb.length() - 1) == '/') {
			sb.setLength(sb.length() - 1);
		}
		final String database = request.getDatabase();
		if (database != null && !database.isEmpty()) {
			sb.append("/_db/").append(database);
		}
		sb.append(request.getRequest());
		if (!request.getQueryParam().isEmpty()) {
			if (request.getRequest().contains("?")) {
				sb.append("&");
			} else {
				sb.append("?");
			}
			final String paramString = URLEncodedUtils.format(toList(request.getQueryParam()), "utf-8");
			sb.append(paramString);
		}
		return sb.toString();
	}

	private HttpRequestBase buildHttpRequestBase(final Request request, final String url) {
		final HttpRequestBase httpRequest;
		switch (request.getRequestType()) {
		case POST:
			httpRequest = requestWithBody(new HttpPost(url), request);
			break;
		case PUT:
			httpRequest = requestWithBody(new HttpPut(url), request);
			break;
		case PATCH:
			httpRequest = requestWithBody(new HttpPatch(url), request);
			break;
		case DELETE:
			httpRequest = requestWithBody(new HttpDeleteWithBody(url), request);
			break;
		case HEAD:
			httpRequest = new HttpHead(url);
			break;
		case GET:
		default:
			httpRequest = new HttpGet(url);
			break;
		}
		return httpRequest;
	}

	private HttpRequestBase requestWithBody(final HttpEntityEnclosingRequestBase httpRequest, final Request request) {

		final String jsonBody = request.getJsonBody();

		if (this.contentType == Protocol.HTTP_VPACK) {
			VPackSlice body = request.getBody();
			if (body == null && jsonBody != null) {
				body = this.util.serialize(jsonBody);
			}
			if (body != null) {
				httpRequest.setEntity(new ByteArrayEntity(
						Arrays.copyOfRange(body.getBuffer(), body.getStart(), body.getStart() + body.getByteSize()),
						CONTENT_TYPE_VPACK));
			}
		} else {
			httpRequest.setEntity(new StringEntity(jsonBody, CONTENT_TYPE_APPLICATION_JSON_UTF8));
		}

		return httpRequest;
	}

	private String buildBaseUrl(final HostDescription host, final String path) {
		final StringBuilder sb = new StringBuilder();
		if (Boolean.TRUE == this.useSsl) {
			sb.append("https://");
		} else {
			sb.append("http://");
		}
		sb.append(host.getHost());
		sb.append(':');
		sb.append(host.getPort());
		if (!path.isEmpty()) {
			if (path.charAt(0) != '/') {
				sb.append('/');
			}
			sb.append(path);
		}
		return sb.toString();
	}

	private static List<NameValuePair> toList(final Map<String, String> parameters) {
		final ArrayList<NameValuePair> paramList = new ArrayList<>(parameters.size());
		for (final Entry<String, String> param : parameters.entrySet()) {
			if (param.getValue() != null) {
				paramList.add(new BasicNameValuePair(param.getKey(), param.getValue()));
			}
		}
		return paramList;
	}

	public Response execute(final Request request) throws ArangoDBException, IOException {
		final String url = buildUrl(this.baseUrl, request);
		final HttpRequestBase httpRequest = buildHttpRequestBase(request, url);
		httpRequest.setHeader("User-Agent", "Mozilla/5.0 (compatible; ArangoDB-JavaDriver/1.1; +http://mt.orz.at/)");
		if (this.contentType == Protocol.HTTP_VPACK) {
			httpRequest.setHeader("Accept", "application/x-velocypack");
		}
		addHeader(request, httpRequest);
		final Credentials credentials = addCredentials(httpRequest);
		if (LOGGER.isDebugEnabled()) {
			CURLLogger.log(url, request, credentials, this.util);
		}
		Response response;
		response = buildResponse(this.client.execute(httpRequest));
		checkError(response);
		return response;
	}

	private static void addHeader(final Request request, final HttpRequestBase httpRequest) {
		for (final Entry<String, String> header : request.getHeaderParam().entrySet()) {
			httpRequest.addHeader(header.getKey(), header.getValue());
		}
	}

	public Credentials addCredentials(final HttpRequestBase httpRequest) {
		Credentials credentials = null;
		if (this.user != null) {
			credentials = new UsernamePasswordCredentials(this.user, this.password != null ? this.password : "");
			try {
				httpRequest.addHeader(new BasicScheme().authenticate(credentials, httpRequest, null));
			} catch (final AuthenticationException e) {
				throw new ArangoDBException(e);
			}
		}
		return credentials;
	}

	public Response buildResponse(final CloseableHttpResponse httpResponse)
			throws UnsupportedOperationException, IOException {
		final Response response = new Response();
		response.setResponseCode(httpResponse.getStatusLine().getStatusCode());
		final HttpEntity entity = httpResponse.getEntity();
		if (entity != null && entity.getContent() != null) {
			if (this.contentType == Protocol.HTTP_VPACK) {
				final byte[] content = IOUtils.toByteArray(entity.getContent());
				if (content.length > 0) {
					response.setBody(new VPackSlice(content));
				}
			} else {
				final String content = IOUtils.toString(entity.getContent());
				if (!content.isEmpty()) {
					response.setJsonBody(content, b->this.util.serialize(b, new Options().stringAsJson(true).serializeNullValues(true)));
				}
			}
		}
		final Header[] headers = httpResponse.getAllHeaders();
		final Map<String, String> meta = response.getMeta();
		for (final Header header : headers) {
			meta.put(header.getName(), header.getValue());
		}
		return response;
	}

	protected void checkError(final Response response) throws ArangoDBException {
		ResponseUtils.checkError(this.util, response);
	}

}
