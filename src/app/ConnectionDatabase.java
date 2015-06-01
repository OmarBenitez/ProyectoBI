package app;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Date;
import java.text.SimpleDateFormat;

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
    private static final String USERNAME = "dw";
    private static final String PASSWORD = "12345678";

    public static final String DEFAULT_DATE_FORMAT = "dd/MM/yyyy";

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
     * @param year
     * @return
     * @throws SQLException
     */
    public ResultSet getConsulta1(String year) throws SQLException {
        return st.executeQuery(
                String.format("SELECT pr.PRODUCT_NAME, COUNT(pr.PRODUCT_ID), SUM(ve.SALE_TOTAL) TOTAL_VENDIDO \n"
                        + "FROM PRODUCTOS pr\n"
                        + "FULL OUTER JOIN TIEMPO ti\n"
                        + "FULL OUTER JOIN VENTAS ve\n"
                        + "ON ti.FECHA = ve.FECHA\n"
                        + "ON pr.PRODUCT_ID = ve.PRODUCT_ID\n"
                        + "WHERE ti.YEAR_NUMBER = %s\n"
                        + "GROUP BY ROLLUP (pr.PRODUCT_NAME)",
                        year
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

        SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT);

        return st.executeQuery(String.format("SELECT CLIENT_NAME, COUNT(SALE_ID)\n"
                + "FROM VENTAS ve\n"
                + "JOIN CLIENTES cl\n"
                + "ON ve.CLIENT_ID = cl.CLIENT_ID\n"
                + "WHERE TO_DATE(ve.FECHA, '%s') BETWEEN TO_DATE('%s', '%s')\n"
                + "AND TO_DATE('%s', '%s')\n"
                + "GROUP BY GROUPING SETS(CLIENT_NAME)",
                DEFAULT_DATE_FORMAT,
                sdf.format(desde),
                DEFAULT_DATE_FORMAT,
                sdf.format(hasta),
                DEFAULT_DATE_FORMAT));
    }

    /**
     * Comparación de cantidad de ventas entre clientes.
     *
     * @param costumer_id
     * @return
     * @throws SQLException
     */
    ResultSet getConsulta6() throws SQLException {
        return st.executeQuery(String.format("SELECT cl.CLIENT_NAME, SUM(ve.SALE_TOTAL) TOTAL_VENDIDO, \n"
                + "LAG (cl.CLIENT_NAME) OVER (ORDER BY SUM(ve.SALE_TOTAL)) AS VENDEDOR_DESEMPEÑO_MENOR, \n"
                + "LEAD (cl.CLIENT_NAME) OVER (ORDER BY SUM(ve.SALE_TOTAL)) AS VENDEDOR_DESEMPEÑO_MAYOR\n"
                + "FROM VENTAS ve\n"
                + "JOIN CLIENTES cl\n"
                + "ON ve.CLIENT_ID = cl.CLIENT_ID\n"
                + "GROUP BY cl.CLIENT_NAME;"));
    }

    /**
     * Los productos más vendidos en un almacén.
     *
     * @param numAlmacen
     * @return
     * @throws SQLException
     */
    ResultSet getConsulta7(Object numAlmacen) throws SQLException {
        return st.executeQuery(String.format("SELECT al.WAREHOUSE_NAME, pr.PRODUCT_NAME, COUNT(ve.PRODUCT_ID)\n"
                + "FROM VENTAS ve\n"
                + "JOIN PRODUCTOS pr\n"
                + "ON ve.PRODUCT_ID = pr.PRODUCT_ID\n"
                + "JOIN ALMACENES al\n"
                + "ON al.WAREHOUSE_ID = al.WAREHOUSE_ID\n"
                + "WHERE al.WAREHOUSE_ID = %s\n"
                + "GROUP BY GROUPING SETS(al.WAREHOUSE_NAME, pr.PRODUCT_NAME)\n"
                + "ORDER BY COUNT(ve.PRODUCT_ID) DESC", numAlmacen));
    }

    /**
     * Total de las ventas por mes para cada uno de los clientes.
     *
     * @param day
     * @param desde
     * @param hasta
     * @return
     * @throws SQLException
     */
    ResultSet getConsulta8(String month) throws SQLException {
        return st.executeQuery(String.format("SELECT cl.CLIENT_NAME, SUM(ve.SALE_TOTAL) TOTAL_VENTAS, ti.MONTH_NAME as MES\n"
                + "FROM TIEMPO ti\n"
                + "JOIN VENTAS ve\n"
                + "ON ve.FECHA = ti.FECHA\n"
                + "JOIN CLIENTES cl\n"
                + "ON ve.CLIENT_ID = cl.CLIENT_ID\n"
                + "WHERE ti.MONTH_NAME LIKE '%s'\n"
                + "GROUP BY GROUPING SETS ((cl.CLIENT_NAME, ti.MONTH_NAME))", month));
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
     * Cantidad de ventas por país por año.
     *
     * @param country
     * @return
     * @throws SQLException
     */
    ResultSet getConsulta10(Object country) throws SQLException {
        return st.executeQuery(String.format("SELECT cl.CLIENT_COUNTRY, ti.YEAR_NUMBER, MAX(ve.SALE_TOTAL) "
                + "FROM CLIENTES cl "
                + "JOIN TIEMPO ti "
                + "JOIN VENTAS ve "
                + "ON ti.FECHA = ve.FECHA "
                + "ON cl.CLIENT_ID = ve.CLIENT_ID "
                + "WHERE cl.CLIENT_COUNTRY LIKE '%s' "
                + "GROUP BY ROLLUP (cl.CLIENT_COUNTRY, ti.YEAR_NUMBER)", country.toString()));
    }

}
