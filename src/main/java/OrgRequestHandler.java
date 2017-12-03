
import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;


class OrgRequestHandler implements HttpHandler {


    public void handle(HttpExchange exchange) throws IOException {

       System.out.println( exchange.getRequestURI());
        ArrayList<Organization> organizations = null;
        try {
            organizations = OrganizationDB.getInstance().selectOrganizations(exchange.getRequestURI().getQuery());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        Gson gson = new Gson();
        String jsonResult = "{ \"organizations\": ";
        jsonResult += gson.toJson(organizations);
        jsonResult += "}";

        byte[] response = jsonResult.getBytes();
        exchange.sendResponseHeaders(200, response.length);
        OutputStream os = exchange.getResponseBody();
        os.write(response);
        os.close();

    }
}
