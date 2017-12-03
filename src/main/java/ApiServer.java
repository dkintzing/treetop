
/*
 * ApiServer - starts an http server with one endpoint /organizations
 */

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.sql.SQLException;

public class ApiServer {


    public static void main (String[] args) {

        try {
            OrganizationDB organizationDB = OrganizationDB.getInstance();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
            final HttpContext context = server.createContext("/organizations", new OrgRequestHandler());
            server.setExecutor(null);
            server.start();
        }
        catch (BindException be) {
            //log error
        }
        catch (IOException e) {
            //log error
        }
    }
}