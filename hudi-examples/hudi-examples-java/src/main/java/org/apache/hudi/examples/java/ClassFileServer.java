package org.apache.hudi.examples.java;

/* ClassFileServer.java -- a simple file server that can server
 * Http get request in both clear and secure channel
 *
 * The ClassFileServer implements a ClassServer that
 * reads files from the file system. See the
 * doc for the "Main" method for how to run this
 * server.
 */

import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;

public class ClassFileServer extends ClassServer {

  public static final int SERVER_PORT = 2001;
  public static final boolean USE_SSL = false;
  public static final boolean NEED_CLIENT_AUTH = false;
  public static final String SSL_KEYSTORE_LOCATION_PROP = "/Users/jon/Documents/ssl_example/server2/KeyStore.jks";
  public static final String SSL_TRUSTSTORE_LOCATION_PROP = "/Users/jon/Documents/ssl_example/server2/truststore.jks";
  public static final String SSL_KEYSTORE_PASSWORD_PROP = "serverkeystore";
  public static final String SSL_TRUSTSTORE_PASSWORD_PROP = "servertruststore";
  public static final String SSL_KEY_PASSWORD_PROP = "bmcpass";

  public ClassFileServer(ServerSocket ss) throws IOException {
    super(ss);
  }

  /**
   * Main method to create the class server that reads
   * files. This takes two command line arguments, the
   * port on which the server accepts requests and the
   * root of the path. To start up the server: <br><br>
   *
   * <code>   java ClassFileServer <port>
   * </code><br><br>
   *
   * <code>   new ClassFileServer(port);
   * </code>
   */
  public static void main(String[] args) {

    try {
      ServerSocketFactory ssf =
          ClassFileServer.getServerSocketFactory();
      assert ssf != null;
      ServerSocket ss = ssf.createServerSocket(SERVER_PORT);
      if (NEED_CLIENT_AUTH) {
        ((SSLServerSocket)ss).setNeedClientAuth(true);
      }
      new ClassFileServer(ss);
    } catch (IOException e) {
      System.out.println("Unable to start ClassServer: " + e.getMessage());
      e.printStackTrace();
    }
  }

  private static ServerSocketFactory getServerSocketFactory() {
    SSLServerSocketFactory ssf;
    if (USE_SSL) {
      try {
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
        ssf = sslContextBuilder.build().getServerSocketFactory();
        return ssf;
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      return ServerSocketFactory.getDefault();
    }
    return null;
  }
}
