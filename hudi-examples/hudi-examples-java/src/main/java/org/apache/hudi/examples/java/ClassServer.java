package org.apache.hudi.examples.java;

/*
 * ClassServer.java -- a simple file server that can serve
 * Http get request in both clear and secure channel
 */

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public abstract class ClassServer implements Runnable {

  private ServerSocket server;
  /**
   * Constructs a ClassServer based on <b>ss</b> and
   * obtains a file's bytecodes using the method <b>getBytes</b>.
   *
   */
  protected ClassServer(ServerSocket ss) {
    server = ss;
    newListener();
  }

  /**
   * The "listen" thread that accepts a connection to the
   * server, parses the header to obtain the file name
   * and sends back the bytes for the file (or error
   * if the file is not found or the response was malformed).
   */
  public void run() {
    Socket socket;

    // accept a connection
    try {
      socket = server.accept();
    } catch (IOException e) {
      System.out.println("Class Server died: " + e.getMessage());
      e.printStackTrace();
      return;
    }

    // create a new thread to accept the next connection
    newListener();

    try {
      OutputStream rawOut = socket.getOutputStream();

      PrintWriter out = new PrintWriter(
          new BufferedWriter(
              new OutputStreamWriter(
                  rawOut)));
      try {
        String sampleJSON = "{\n    \"schema\": \"Data Sent Successfully\"\n}\n";
        // retrieve bytecodes
        byte[] bytecodes = sampleJSON.getBytes();
        // send bytecodes in response (assumes HTTP/1.0 or later)
        try {
          out.print("HTTP/1.0 200 OK\r\n");
          out.print("Content-Length: " + bytecodes.length + "\r\n");
          out.print("Content-Type: text/html\r\n\r\n");
          out.flush();
          rawOut.write(bytecodes);
          rawOut.flush();
        } catch (IOException ie) {
          ie.printStackTrace();
          return;
        }

      } catch (Exception e) {
        e.printStackTrace();
        // write out error response
        out.println("HTTP/1.0 400 " + e.getMessage() + "\r\n");
        out.println("Content-Type: text/html\r\n\r\n");
        out.flush();
      }

    } catch (IOException ex) {
      // eat exception (could log error to log file, but
      // write out to stdout for now).
      System.out.println("error writing response: " + ex.getMessage());
      ex.printStackTrace();

    } finally {
      try {
        socket.close();
      } catch (IOException e) {
        System.out.println(("error closing socket: ") + e.getMessage());
      }
    }
  }

  /**
   * Create a new thread to listen.
   */
  private void newListener() {
    (new Thread(this)).start();
  }
}