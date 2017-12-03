

import org.apache.derby.jdbc.EmbeddedDriver;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;


/*
 *  OrganizationDB populates an in memory database with organizations
 *  and handles selecting organizations based on the api query parameters
 */
public class OrganizationDB {

    private static OrganizationDB organizationDB = null;
    private Statement statement;
    PreparedStatement preparedStatement;
    Connection connection;

    public static OrganizationDB getInstance() throws SQLException {
        if (organizationDB == null) {
            organizationDB = new OrganizationDB();
        }

        return organizationDB;
    }

    private OrganizationDB() throws SQLException {
        Driver derbyDriver = new EmbeddedDriver();
        DriverManager.registerDriver(derbyDriver);

        connection = DriverManager.getConnection("jdbc:derby:memory:treetop;create=true");

        //create organizations table
        statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        String createTable = "create table organizations ("
                + "id integer not null, "
                + "name varchar (200), "
                + "city varchar (100), "
                + "state char (2), "
                + "postal varchar (50), "
                + "category varchar (100))";
        statement.execute(createTable);


        //load rows from the provided file removing duplicates
        try {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("organization_sample_data.csv")));
            String line;
            while((line = bufferedReader.readLine()) != null) {
                if(line.contains("id,name,city,state,postal,category")) {
                    continue;
                }
                String [] fields = line.split(",");

                String name = fields[1];
                int extra = 0;
                if(fields.length > 6) {
                    extra = fields.length - 6;
                    name = fields[1];
                    for(int i = 1; i <= extra; i++) {
                        name += fields[i + 1];
                    }
                }

                if(name.equalsIgnoreCase("null")) {
                    name = "unknown";
                }

                String city = fields[2];
                if(extra > 0) {
                    city = fields[2 + extra];
                }
                if(city.equalsIgnoreCase("null")) {
                    city = "unknown";
                }
                String state = fields[3];
                if(extra > 0) {
                    state = fields[3 + extra];
                }
                if(state.equalsIgnoreCase("null")) {
                    state = "uk";
                }
                String postal = fields[4];
                if(extra > 0) {
                    postal = fields[4 + extra];
                }
                if(postal.equalsIgnoreCase("null")) {
                    postal = "unknown";
                }
                String category = fields[5];
                if(extra > 0) {
                    category = fields[5 + extra];
                }
                if(category.equalsIgnoreCase("null")) {
                    category = "unknown";
                }

                preparedStatement = connection.prepareStatement(
                        "insert into organizations "
                        + "(id, name, city, state, postal, category) "
                        + "values (?,?,?,?,?,?)");
                preparedStatement.setInt(1, Integer.parseInt(fields[0]));
                preparedStatement.setString(2, name);
                preparedStatement.setString(3, city);
                preparedStatement.setString(4, state);
                preparedStatement.setString(5, postal);
                preparedStatement.setString(6, category);

                //check for duplicate
                boolean duplicate = checkForDuplicate(name,city,state,postal,category);
                if (!duplicate) {
                    preparedStatement.executeUpdate();
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

    }

    /*
     * checkForDuplicate is uses when loading organizations to see if we have a duplicate
     */
    private boolean checkForDuplicate(String name, String city, String state, String postal, String category) throws SQLException {

        PreparedStatement statement = connection.prepareStatement("select name,city,state,postal,category from organizations where name = ?"
                +  " and city = ? and state = ? and postal = ?"
                + " and category = ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        statement.setString(1, name);
        statement.setString(2, city);
        statement.setString(3, state);
        statement.setString(4, postal);
        statement.setString(5, category);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.first()) {
            return true;
        }
        else {
            return false;
        }
    }



    /*
     * select Organizations takes the query parameters and returns
     * and array of organization objects that match the query selection.
     */
    public ArrayList<Organization> selectOrganizations(String query) throws SQLException {
        ArrayList<Organization> organizations = new ArrayList<Organization>();
        String name = null;
        String city = null;
        String state = null;
        String postal = null;
        String category = null;
        boolean orderby = false;
        String orderField = null;
        String direction = "ASC";

        boolean hasQuery = true;
        boolean hasValidQuery = false;

        //step one is to determine which query parameters we received
        if(query == null) {
            hasQuery = false;
        }
        else {
            String [] params = query.split("&");

            for(int i = 0; i < params.length; i++) {
                if(params[i].startsWith("Name")) {
                    name = params[i].split("=")[1];
                    hasValidQuery = true;
                }
                else if(params[i].startsWith("City")) {
                    city = params[i].split("=")[1];
                    hasValidQuery = true;
                }
                else if(params[i].startsWith("State")) {
                    state = params[i].split("=")[1];
                    hasValidQuery = true;
                }
                else if(params[i].startsWith("Postal")) {
                    postal = params[i].split("=")[1];
                    hasValidQuery = true;
                }
                else if(params[i].startsWith("Category")) {
                    category = params[i].split("=")[1];
                    hasValidQuery = true;
                }
                else if(params[i].startsWith("Orderby")) {
                    orderby = true;
                    orderField = params[i].split("=")[1];
                }
                else if(params[i].startsWith("Direction")) {
                    direction = params[i].split("=")[1];
                }
            }

        } //end if query


        //next step is to get the value for any parameters we received and build a where clause at the same time
        boolean started = false;
        int nameParameter = 0;
        int cityParameter = 0;
        int stateParameter = 0;
        int postalParameter = 0;
        int categoryParameter = 0;
        int parameterCount = 0;
        String whereClause = "where ";
        if(name != null) {
            started = true;
            whereClause += "name = ?";
            parameterCount++;
            nameParameter = parameterCount;
        }
        if(city != null) {
            if(started) {
                whereClause += " and city = ?";
            }
            else {
                whereClause = "city = ?";
                started = true;
            }
            parameterCount++;
            cityParameter = parameterCount;
        }
        if(state != null) {
            if(started) {
                whereClause += " and state = ?";
            }
            else {
                whereClause = "state = ?";
                started = true;
            }
            parameterCount++;
            stateParameter = parameterCount;
        }
        if(postal != null) {
            if(started) {
                whereClause += " and postal = ?";
            }
            else {
                whereClause += "postal = ?";
                started = true;
            }
            parameterCount++;
            postalParameter = parameterCount;
        }
        if(category != null) {
            if(started) {
                whereClause += " and category = ?";
            }
            else {
                whereClause += "category = ?";
            }
            parameterCount++;
            categoryParameter = parameterCount;
        }
        if(orderby) {
            whereClause += " order by " + orderField + " ";

            if(direction.equalsIgnoreCase("ASC")) {
                whereClause += "asc";
            }
            else {
                whereClause += "desc";
            }
        }

        //now put together our query and run it
        PreparedStatement statement = null;
        String sqlQuery = "select * from organizations ";
        if(hasQuery && hasValidQuery) {
            sqlQuery += whereClause;
            statement = connection.prepareStatement(sqlQuery, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);


            if (name != null) {
                statement.setString(nameParameter, name);
            }
            if (city != null) {
                statement.setString(cityParameter, city);
            }
            if (state != null) {
                statement.setString(stateParameter, state);
            }
            if (postal != null) {
                statement.setString(postalParameter, postal);
            }
            if (category != null) {
                statement.setString(categoryParameter, category);
            }
        }
        else if(hasQuery && !hasValidQuery) {
            return null;
        }
        else {
            if(orderby) {
                sqlQuery += " order by " + orderField + " ";

                if(direction.equalsIgnoreCase("ASC")) {
                    sqlQuery += "asc";
                }
                else {
                    sqlQuery += "desc";
                }
            }
            statement = connection.prepareStatement(sqlQuery, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        }

        ResultSet resultSet = statement.executeQuery();

        //create organization objects from the query results and add then to an array
        while(resultSet.next()) {
            int rid = resultSet.getInt("id");
            String rname = resultSet.getString("name");
            String rcity = resultSet.getString("city");
            String rstate = resultSet.getString("state");
            String rpostal = resultSet.getString("postal");
            String rcategory = resultSet.getString("category");

            Organization org = new Organization();
            org.setId(rid);
            org.setName(rname);
            org.setCity(rcity);
            org.setState(rstate);
            org.setPostal(rpostal);
            org.setCategory(rcategory);
            organizations.add(org);

        }  //end while resultSet

        return organizations;

    } //end selectOrganizations

} //end class
