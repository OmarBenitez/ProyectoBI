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
     * @param limit
     * @return
     * @throws SQLException
     */
    public ResultSet getConsulta1(Date desde, Date hasta, Integer limit) throws SQLException {
        int dummyYear = 2007;
        return st.executeQuery(
                String.format("SELECT pr.PRODUCT_NAME, COUNT(pr.PRODUCT_ID), SUM(ve.SALE_TOTAL) TOTAL_VENDIDO  "
                        + "FROM PRODUCTOS pr "
                        + "FULL OUTER JOIN TIEMPO ti "
                        + "FULL OUTER JOIN VENTAS ve "
                        + "ON ti.FECHA = ve.FECHA "
                        + "ON pr.PRODUCT_ID = ve.PRODUCT_ID "
                        + "WHERE ti.YEAR_NUMBER = %d "
                        + "GROUP BY ROLLUP (pr.PRODUCT_NAME)",
                        dummyYear
                )
        );
    }

    /**
     * Categoria de productos más vendidos de una fecha a otra.
     *
     * @param desde
     * @param hasta
     * @return
     * @throws SQLException
     */
    public ResultSet getConsulta2(Date desde, Date hasta) throws SQLException {

        SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT);

        return st.executeQuery(String.format("SELECT pr.CATEGORY, COUNT(ve.PRODUCT_ID) "
                + "FROM VENTAS ve "
                + "JOIN PRODUCTOS pr "
                + "ON ve.PRODUCT_ID = pr.PRODUCT_ID "
                + "WHERE TO_DATE(ve.FECHA, '%s') BETWEEN TO_DATE('%s', '%s') "
                + "AND TO_DATE('%s', '%s') "
                + "GROUP BY GROUPING SETS(pr.CATEGORY) "
                + "ORDER BY COUNT(ve.PRODUCT_ID) DESC",
                DEFAULT_DATE_FORMAT,
                sdf.format(desde),
                DEFAULT_DATE_FORMAT,
                sdf.format(hasta),
                DEFAULT_DATE_FORMAT));
    }

    /**
     * Clientes con más compras de un producto por país.
     *
     * @param product_id
     * @return
     * @throws SQLException
     */
    ResultSet getConsulta3(Object product_id) throws SQLException {
        return st.executeQuery(String.format("SELECT cl.CLIENT_NAME, cl.CLIENT_COUNTRY, COUNT(ve.PRODUCT_ID) "
                + "FROM VENTAS ve "
                + "JOIN CLIENTES cl "
                + "ON ve.CLIENT_ID = cl.CLIENT_ID "
                + "WHERE ve.PRODUCT_ID = %s "
                + "GROUP BY CUBE(cl.CLIENT_NAME, cl.CLIENT_COUNTRY)",
                product_id));
    }

    /**
     * Productos con más venta en un pais.
     *
     * @param country
     * @return
     * @throws SQLException
     */
    ResultSet getConsulta4(Object country) throws SQLException {
        return st.executeQuery(String.format("SELECT pr.PRODUCT_NAME PRODUCTO, COUNT(pr.PRODUCT_ID) CANTIDAD, cl.CLIENT_COUNTRY "
                + "FROM PRODUCTOS pr "
                + "JOIN VENTAS ve "
                + "ON ve.PRODUCT_ID = pr.PRODUCT_ID "
                + "JOIN CLIENTES cl "
                + "ON cl.CLIENT_ID = ve.CLIENT_ID "
                + "WHERE cl.CLIENT_COUNTRY like '%s' "
                + "GROUP BY ROLLUP (cl.CLIENT_COUNTRY, pr.PRODUCT_NAME) "
                + "ORDER BY COUNT(pr.PRODUCT_ID) DESC", country));
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

        return st.executeQuery(String.format("SELECT CLIENT_NAME, COUNT(SALE_ID) "
                + "FROM VENTAS ve "
                + "JOIN CLIENTES cl "
                + "ON ve.CLIENT_ID = cl.CLIENT_ID "
                + "WHERE TO_DATE(ve.FECHA, '%s') BETWEEN TO_DATE('%s', '%s') "
                + "AND TO_DATE('%s', '%s') "
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
    ResultSet getConsulta6(Object costumer_id) throws SQLException {
        //No recibe parámetros
        return st.executeQuery(String.format("SELECT cl.CLIENT_NAME, SUM(ve.SALE_TOTAL) TOTAL_VENDIDO,  "
                + "LAG (cl.CLIENT_NAME) OVER (ORDER BY SUM(ve.SALE_TOTAL)) AS VENDEDOR_DESEMPEÑO_MENOR,  "
                + "LEAD (cl.CLIENT_NAME) OVER (ORDER BY SUM(ve.SALE_TOTAL)) AS VENDEDOR_DESEMPEÑO_MAYOR "
                + "FROM VENTAS ve "
                + "JOIN CLIENTES cl "
                + "ON ve.CLIENT_ID = cl.CLIENT_ID "
                + "GROUP BY cl.CLIENT_NAME"));
    }

    /**
     * Los productos más vendidos en un almacén.
     *
     * @param numAlmacen
     * @return
     * @throws SQLException
     */
    ResultSet getConsulta7(Object numAlmacen) throws SQLException {
        return st.executeQuery(String.format("SELECT al.WAREHOUSE_NAME, pr.PRODUCT_NAME, COUNT(ve.PRODUCT_ID) "
                + "FROM VENTAS ve "
                + "JOIN PRODUCTOS pr "
                + "ON ve.PRODUCT_ID = pr.PRODUCT_ID "
                + "JOIN ALMACENES al "
                + "ON al.WAREHOUSE_ID = al.WAREHOUSE_ID "
                + "WHERE al.WAREHOUSE_ID = %s "
                + "GROUP BY GROUPING SETS(al.WAREHOUSE_NAME, pr.PRODUCT_NAME) "
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
    ResultSet getConsulta8(Date desde, Date hasta, Integer day) throws SQLException {
        String dummyMonth = "Ënero";
        return st.executeQuery(String.format("SELECT cl.CLIENT_NAME, SUM(ve.SALE_TOTAL) TOTAL_VENTAS, ti.MONTH_NAME as MES "
                + "FROM TIEMPO ti "
                + "JOIN VENTAS ve "
                + "ON ve.FECHA = ti.FECHA "
                + "JOIN CLIENTES cl "
                + "ON ve.CLIENT_ID = cl.CLIENT_ID "
                + "WHERE ti.MONTH_NAME LIKE '%s' "
                + "GROUP BY GROUPING SETS ((cl.CLIENT_NAME, ti.MONTH_NAME))", dummyMonth));
    }

    /**
     * Cuanto tiempo se queda inexistente un producto.
     *
     * @param product_id
     * @return
     * @throws SQLException
     */
    ResultSet getConsulta9(Object product_id) throws SQLException {
        return st.executeQuery(String.format("SELECT pr.CATEGORY, COUNT(ve.PRODUCT_ID) "
                + "FROM VENTAS ve "
                + "JOIN PRODUCTOS pr "
                + "ON ve.PRODUCT_ID = pr.PRODUCT_ID "
                + "JOIN CLIENTES cl  "
                + "ON ve.CLIENT_ID = cl.CLIENT_ID "
                + "WHERE cl.CLIENT_COUNTRY LIKE '%s' "
                + "GROUP BY GROUPING SETS(pr.CATEGORY) "
                + "ORDER BY COUNT(ve.PRODUCT_ID) DESC", product_id));
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
