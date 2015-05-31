package app;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import oracle.jdbc.driver.OracleDriver;

/**
 *
 * @author Team Burton
 */
public class ConnectionDatabase {

    public static Connection con;
    public static Statement st;
    public static ResultSet rs;

    private static final String DRIVER_NAME = "jdbc:oracle:thin:";
    private static final String HOST = "192.168.100.6";
    private static final String PORT = "1521";
    private static final String DB_NAME = "orcl";
    private static final String USERNAME = "oe";
    private static final String PASSWORD = "12346678";

    public void conectar() {
//Conectar con la base de datos
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");

            con = DriverManager.getConnection(
                    String.format("%s@%s:%s/%s", DRIVER_NAME, HOST, PORT, DB_NAME), 
                    USERNAME,
                    PASSWORD);
            st = con.createStatement();
            System.out.println("Conectado a la Base de Datos.");
        } catch (SQLException | ClassNotFoundException e) {
            System.out.println("Asegurese que tenga conectividad con la base de datos.");
            e.printStackTrace(System.err);
        }
    }

    public ResultSet select(String query) throws SQLException {
//Método simple para realizar un select en la base de datos

//Obtención del resultado de la consulta
        this.rs = this.st.executeQuery(query);

//Se devuelve el rs
        return this.rs;
    }

    public int update(String query) throws SQLException {
//Método como alias para insert (ya que ambos utilizan executeUpdate)
        return insert(query);
    }

    public int insert(String query) throws SQLException {
//Devuelve el resultado del insert/update
        return this.st.executeUpdate(query);
    }

    public boolean execute(String sql) throws SQLException {
        return st.execute(sql);
    }
}
