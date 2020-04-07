/*
 * DISCLAIMER
 *
 * Copyright 2016 ArangoDB GmbH, Cologne, Germany
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

package com.arangodb.velocystream;

import java.util.HashMap;
import java.util.Map;

import com.arangodb.velocypack.VPackSlice;
import com.arangodb.velocypack.annotations.Expose;

/**
 * @author Mark Vollmary
 */
public class Request {

	private int version = 1;
	private int type = 1;
	private final String database;
	private final RequestType requestType;
	private final String request;
	private final Map<String, String> queryParam;
	private final Map<String, String> headerParam;
	@Expose(serialize = false)
	private VPackSlice body;
	@Expose(serialize = false)
	private String jsonBody="";

	public Request(final String database, final RequestType requestType, final String path) {
		super();
		this.database = database;
		this.requestType = requestType;
		this.request = path;
		this.body = null;
		this.queryParam = new HashMap<>();
		this.headerParam = new HashMap<>();
	}

	public int getVersion() {
		return this.version;
	}

	public Request setVersion(final int version) {
		this.version = version;
		return this;
	}

	public int getType() {
		return this.type;
	}

	public Request setType(final int type) {
		this.type = type;
		return this;
	}

	public String getDatabase() {
		return this.database;
	}

	public RequestType getRequestType() {
		return this.requestType;
	}

	public String getRequest() {
		return this.request;
	}

	public Map<String, String> getQueryParam() {
		return this.queryParam;
	}

	public Request putQueryParam(final String key, final Object value) {
		if (value != null) {
			this.queryParam.put(key, value.toString());
		}
		return this;
	}

	public Map<String, String> getHeaderParam() {
		return this.headerParam;
	}

	public Request putHeaderParam(final String key, final String value) {
		if (value != null) {
			this.headerParam.put(key, value);
		}
		return this;
	}

	public VPackSlice getBody() {
		return this.body;
	}

	public Request setBody(final VPackSlice body) {
		this.body = body;
		return this;
	}

	public Request setJsonBody(String jsonBody) {
		this.jsonBody = jsonBody;
		return this;
	}

	public String getJsonBody() {
		if(this.body!=null && this.jsonBody.isEmpty()) {
			this.jsonBody = this.body.toString();
		}
		return this.jsonBody;
	}

}
