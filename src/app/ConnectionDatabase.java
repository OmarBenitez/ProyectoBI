package app;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Date;

/**
 *
 * @author Team Burton
 */
public class ConnectionDatabase {

    public static Connection con;
    public static Statement st;
    public static ResultSet rs;

    private static final String DRIVER_NAME = "jdbc:oracle:thin:";
    private static final String HOST = "localhost";
    private static final String PORT = "1521";
    private static final String DB_NAME = "orcl.lan";
    private static final String USERNAME = "oe";
    private static final String PASSWORD = "12345678";

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

    ResultSet getProductos() throws SQLException {
        return st.executeQuery("SELECT * FROM productos");
    }

    ResultSet getCountries() throws SQLException {
        return st.executeQuery("SELECT DISTINCT client_country FROM clientes");
    }

    ResultSet getCostumers() throws SQLException {
        return st.executeQuery("SELECT * FROM clientes");
    }

    /**
     * Los primeros productos mas vendidos de una fecha a otra.
     *
     * @param desde
     * @param hasta
     * @param limit
     * @return
     * @throws SQLException
     */
    public ResultSet getConsulta1(Date desde, Date hasta, Integer limit) throws SQLException {
        return st.executeQuery(
                String.format("SELECT * FROM ventas WHERE sale_date BETWEEN %s AND %s GROUP BY product_id LIMIT %d",
                        desde,
                        hasta,
                        limit
                )
        );
    }

    /**
     * Categoria de productos mas vendidos de una fecha a otra.
     *
     * @param desde
     * @param hasta
     * @return
     * @throws SQLException
     */
    public ResultSet getConsulta2(Date desde, Date hasta) throws SQLException {
        return st.executeQuery("SELECT * FROM employees");
    }

    /**
     * Clientes con más compras de un producto.
     *
     * @param product_id
     * @return
     * @throws SQLException
     */
    ResultSet getConsulta3(Object product_id) throws SQLException {
        return st.executeQuery("SELECT * FROM productos");
    }

    /**
     * Productos con más venta en un pais.
     *
     * @param country
     * @return
     * @throws SQLException
     */
    ResultSet getConsulta4(Object country) throws SQLException {
        return st.executeQuery("SELECT * FROM productos");
    }

    /**
     * Clientes con mas ordenes en un determinado tiempo.
     *
     * @param desde 
     * @param hasta 
     * @return
     * @throws SQLException
     */
    public ResultSet getConsulta5(Date desde, Date hasta) throws SQLException {
        return st.executeQuery("SELECT * FROM employees");
    }

    /**
     * Lapso de tiempo de un cliente en pedir un producto.
     *
     * @param costumer_id
     * @return
     * @throws SQLException
     */
    ResultSet getConsulta6(Object costumer_id) throws SQLException {
        return st.executeQuery("SELECT * FROM productos");
    }

    /**
     * Producto que se queda mas tiempo en el almacén.
     *
     * @param numAlmacen
     * @return
     * @throws SQLException
     */
    ResultSet getConsulta7(Object numAlmacen) throws SQLException {
        return st.executeQuery("SELECT * FROM productos");
    }

    /**
     * Días de la semana con mayor número de ventas en un tiempo determinado.
     *
     * @param day
     * @param desde 
     * @param hasta 
     * @return
     * @throws SQLException
     */
    ResultSet getConsulta8(Date desde, Date hasta, Integer day) throws SQLException {
        return st.executeQuery("SELECT * FROM productos");
    }

    /**
     * Cuanto tiempo se queda inexistente un producto.
     *
     * @param product_id
     * @return
     * @throws SQLException
     */
    ResultSet getConsulta9(Object product_id) throws SQLException {
        return st.executeQuery("SELECT * FROM productos");
    }

    /**
     * Cantidad de ventas por país en un tiempo determinado.
     *
     * @param country
     * @return
     * @throws SQLException
     */
    ResultSet getConsulta10(Object country) throws SQLException {
        return st.executeQuery("SELECT * FROM productos");
    }

}
