package org.apache.hudi.examples.java;

/*
 * This example demostrates how to use a SSLSocket as client to
 * send a HTTP request and get response from an HTTPS server.
 * It assumes that the client is not behind a firewall
 */

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("checkstyle:CommentsIndentation")
public class SSLSocketClient {

  public static final String SSL_KEYSTORE_LOCATION_PROP = "/Users/jon/Documents/ssl_example/client2/KeyStore.jks";
  public static final String SSL_TRUSTSTORE_LOCATION_PROP = "/Users/jon/Documents/ssl_example/client2/truststore.jks";
  public static final String SSL_KEYSTORE_PASSWORD_PROP = "clientkeystore";
  public static final String SSL_TRUSTSTORE_PASSWORD_PROP = "clienttruststore";
  public static final String SSL_KEY_PASSWORD_PROP = "bmcpass";
  public static final boolean USE_SSL = false;

  static {
    //for localhost testing only
    javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(
        new javax.net.ssl.HostnameVerifier() {

          public boolean verify(String hostname,
                                javax.net.ssl.SSLSession sslSession) {
            if (hostname.equals("localhost")) {
              return true;
            }
            return false;
          }
        });
  }

  public static void main(String[] args) throws Exception {
    String serverURL;
    SSLSocketFactory sslSocketFactory = null;
    if (USE_SSL) {
      serverURL = "https://localhost:" + ClassFileServer.SERVER_PORT;
      SSLContextBuilder sslContextBuilder = SSLContexts.custom();
      sslContextBuilder.loadTrustMaterial(
          new File(SSL_TRUSTSTORE_LOCATION_PROP),
          SSL_TRUSTSTORE_PASSWORD_PROP.toCharArray(),
          new TrustSelfSignedStrategy()
      );
      sslContextBuilder.loadKeyMaterial(
          new File(SSL_KEYSTORE_LOCATION_PROP),
          SSL_KEYSTORE_PASSWORD_PROP.toCharArray(),
          SSL_KEY_PASSWORD_PROP.toCharArray()
      );
      sslSocketFactory = sslContextBuilder.build().getSocketFactory();
    } else {
      serverURL = "http://localhost:" + ClassFileServer.SERVER_PORT;
    }

    HttpURLConnection connection;
    Matcher matcher = Pattern.compile("://(.*?)@").matcher(serverURL);
    if (matcher.find()) {
      String creds = matcher.group(1);
      String urlWithoutCreds = serverURL.replace(creds + "@", "");
      connection = getConnection(urlWithoutCreds, sslSocketFactory);
      setAuthorizationHeader(matcher.group(1), connection);
    } else {
      connection = getConnection(serverURL, sslSocketFactory);
    }

    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readTree(getStream(connection));
    System.out.println(node.get("schema").asText());
  }

  protected static HttpURLConnection getConnection(String url, SSLSocketFactory sslSocketFactory) throws IOException {
    URL registry = new URL(url);
    if (sslSocketFactory != null) {
      // we cannot cast to HttpsURLConnection if url is http so only cast when sslSocketFactory is set
      HttpsURLConnection connection = (HttpsURLConnection) registry.openConnection();
      connection.setSSLSocketFactory((sslSocketFactory));
      return connection;
    }
    return (HttpURLConnection) registry.openConnection();
  }

  protected static void setAuthorizationHeader(String creds, HttpURLConnection connection) {
    String encodedAuth = Base64.getEncoder().encodeToString(creds.getBytes(StandardCharsets.UTF_8));
    connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
  }

  protected static InputStream getStream(HttpURLConnection connection) throws IOException {
    return connection.getInputStream();
  }
}
