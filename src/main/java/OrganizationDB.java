

import org.apache.derby.jdbc.EmbeddedDriver;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

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


        //load rows from file
        try {
            String fileLoc = System.getProperty("user.dir") + "/src/main/resources/organization_sample_data.csv";

            File f = new File(fileLoc);
            BufferedReader bufferedReader = new BufferedReader(new FileReader(f));
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

            ResultSet resultSet = statement.executeQuery("select count (*) from organizations");
            resultSet.first();
            System.out.println(resultSet.getInt(1));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

    }

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

        System.out.println("QUERY " + query);
        if(query == null) {
            hasQuery = false;
        }
        else {
            String [] params = query.split("&");

            for(int i = 0; i < params.length; i++) {
                System.out.println("param " + params[i]);
                if(params[i].startsWith("Name")) {
                    System.out.println("FOUND NAME " + params[i]);
                    name = params[i].split("=")[1];
                }
                else if(params[i].startsWith("City")) {
                    System.out.println("FOUND CITY " + params[i]);
                    city = params[i].split("=")[1];
                }
                else if(params[i].startsWith("State")) {
                    System.out.println("FOUND STATE " + params[i]);
                    state = params[i].split("=")[1];
                }
                else if(params[i].startsWith("Postal")) {
                    System.out.println("FOUND POSTAL " + params[i]);
                    postal = params[i].split("=")[1];
                }
                else if(params[i].startsWith("Category")) {
                    System.out.println("FOUND CATEGORY " + params[i]);
                    category = params[i].split("=")[1];
                }
                else if(params[i].startsWith("Orderby")) {
                    System.out.println("FOUND ORDERBY " + params[i]);
                    orderby = true;
                    orderField = params[i].split("=")[1];
                }
                else if(params[i].startsWith("Direction")) {
                    direction = params[i].split("=")[1];
                }
            }

            System.out.println(name);
            System.out.println(city);
            System.out.println(state);
            System.out.println(postal);
            System.out.println(category);
            System.out.println(orderby);
            System.out.println(orderField);
            System.out.println(direction);

        } //end if query

        boolean started = false;
        int nameParameter = 0;
        int cityParameter = 0;
        int stateParameter = 0;
        int postalParameter = 0;
        int categoryParameter = 0;
        int orderByParameter = 0;
        int orderDirection = 0;
        int parameterCount = 0;
        String whereClause = "where ";
        if(name != null) {
            started = true;
            whereClause += "name = ?";
            parameterCount++;
            nameParameter = parameterCount;
            System.out.println("name " + nameParameter);
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
            System.out.println("city " + cityParameter);
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
            System.out.println("state " + stateParameter);
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
            System.out.println("postal " + postalParameter);
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
            System.out.println("category " + categoryParameter) ;
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

        String sqlQuery = "select * from organizations " + whereClause;
        PreparedStatement statement = connection.prepareStatement(sqlQuery, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        System.out.println("WHERE " + whereClause);

        System.out.println(sqlQuery);
        if(hasQuery) {
            //statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
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
        else {
            sqlQuery = "select * from organizations ";
            statement = connection.prepareStatement(sqlQuery
                    , ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        }
        System.out.println("SQL QUERY " + sqlQuery);

        ResultSet resultSet = statement.executeQuery();
//        if(resultSet.first()) {
//            System.out.println("GOT FIRST");
//        }
//        else {
//            System.out.println("NO FIRST");
//        }
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
            System.out.println("ROW " + String.valueOf(rid) + " " + rname + " " + rcity + " " + rstate + " " + rpostal + " " + rcategory);
        }

        return organizations;
    }

}
