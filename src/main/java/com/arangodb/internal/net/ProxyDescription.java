package com.arangodb.internal.net;

public class ProxyDescription {

	private String host;
	private int port;

	private String user;
	private String password;

	public ProxyDescription(String host, int port) {
		super();
		this.host = host;
		this.port = port;
	}


	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public boolean hasAuth() {
		return user != null;
	}
	
	

	public void setUser(String user) {
		this.user = user;
	}


	public void setPassword(String password) {
		this.password = password;
	}


	public String getUser() {
		return user;
	}

	public String getPassword() {
		return password;
	}

	@Override
	public String toString() {
		return "ProxyDescription [ host=" + host + ", port=" + port + "]";
	}

}
