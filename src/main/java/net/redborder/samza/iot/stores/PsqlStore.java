package net.redborder.samza.iot.stores;

import net.redborder.samza.store.WindowStore;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class PsqlStore extends WindowStore {
    private final Logger log = LoggerFactory.getLogger(PsqlStore.class);
    private Connection conn = null;
    private Config config;

    private final String[] enrichColumns = {"campus", "building", "floor", "deployment",
            "namespace", "market", "organization", "service_provider", "campus_uuid",
            "building_uuid", "floor_uuid", "deployment_uuid", "namespace_uuid", "market_uuid",
            "organization_uuid", "service_provider_uuid"};

    public PsqlStore(String name, Config config, KeyValueStore<String, Map<String, Object>> store) {
        super(name, config, store);
        this.config = config;
    }

    @Override
    public void prepare(Config config) {
        if (conn == null) {
            try {
                Class.forName("org.postgresql.Driver");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            String uri = config.get("redborder.postgresql.uri");
            String user = config.get("redborder.postgresql.user");
            String pass = config.get("redborder.postgresql.pass");

            try {
                if (uri != null && user != null) {
                    conn = DriverManager.getConnection(uri, user, pass);
                } else {
                    log.warn("You must specify a URI, user and pass on the config file in order to use postgresql.");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        log.info("Created PSQL connector [{}]", conn);
    }

    @Override
    public Map<String, Map<String, Object>> update() {
        Statement st = null;
        ResultSet rs = null;
        Map<String, Map<String, Object>> result = new HashMap<>();
        try {
            if (conn != null) {
                st = conn.createStatement();
                rs = st.executeQuery("SELECT DISTINCT ON (iot_sensors.uuid) iot_sensors.uuid AS sensor_uuid, iot_sensors.latitude" +
                        " AS latitude, iot_sensors.longitude AS longitude, floors.name AS floor, floors.uuid" +
                        " AS floor_uuid, buildings.name AS building, buildings.uuid AS building_uuid, campuses.name" +
                        " AS campus, campuses.uuid AS campus_uuid, deployments.name AS deployment, deployments.uuid AS" +
                        " deployment_uuid, namespaces.name AS namespace, namespaces.uuid AS namespace_uuid," +
                        " markets.name AS market, markets.uuid AS market_uuid, organizations.name AS organization," +
                        " organizations.uuid AS organization_uuid, service_providers.name AS service_provider," +
                        " service_providers.uuid AS service_provider_uuid FROM iot_sensors JOIN sensors ON iot_sensors.sensor_id" +
                        " = sensors.id LEFT JOIN (SELECT * FROM sensors WHERE domain_type=101) AS floors ON floors.lft" +
                        " <= sensors.lft AND floors.rgt >= sensors.rgt LEFT JOIN (SELECT * FROM sensors WHERE domain_type=5)" +
                        " AS buildings ON buildings.lft <= sensors.lft AND buildings.rgt >= sensors.rgt LEFT JOIN " +
                        "(SELECT * FROM sensors WHERE domain_type=4) AS campuses ON campuses.lft <= sensors.lft AND" +
                        " campuses.rgt >= sensors.rgt LEFT JOIN (SELECT * FROM sensors WHERE domain_type=7) " +
                        "AS deployments ON deployments.lft <= sensors.lft AND deployments.rgt >= sensors.rgt " +
                        "LEFT JOIN (SELECT * FROM sensors WHERE domain_type=8) AS namespaces ON namespaces.lft" +
                        " <= sensors.lft AND namespaces.rgt >= sensors.rgt LEFT JOIN " +
                        "(SELECT * FROM sensors WHERE domain_type=3) AS markets " +
                        "ON markets.lft <= sensors.lft AND markets.rgt >= sensors.rgt LEFT JOIN (SELECT * FROM sensors" +
                        " WHERE domain_type=2) AS organizations ON organizations.lft <= sensors.lft AND " +
                        "organizations.rgt >= sensors.rgt LEFT JOIN (SELECT * FROM sensors WHERE domain_type=6)" +
                        " AS service_providers ON service_providers.lft <= sensors.lft AND service_providers.rgt" +
                        " >= sensors.rgt;");

                while (rs.next()) {
                    Map<String, Object> location = new HashMap<>();
                    Map<String, String> enriching = new HashMap<>();

                    String longitude = rs.getString("longitude");
                    String latitude = rs.getString("latitude");

                    for (String columnName : enrichColumns) {
                        String columnData = rs.getString(columnName);
                        if (columnData != null) enriching.put(columnName, columnData);
                    }

                    if (longitude != null && latitude != null) {
                        Double longitudeDbl = (double) Math.round(Double.valueOf(longitude) * 100000) / 100000;
                        Double latitudeDbl = (double) Math.round(Double.valueOf(latitude) * 100000) / 100000;
                        location.put("client_latlong", latitudeDbl + "," + longitudeDbl);
                    }

                    location.putAll(enriching);

                    if (!location.isEmpty()) {
                        log.debug("IOT: {} LOCATION: {}", rs.getString("sensor_uuid"), location);

                        String sensorUUID = rs.getString("sensor_uuid");

                        if (sensorUUID != null) {
                            result.put(sensorUUID, location);
                        }
                    }
                }

                log.info("PostgreSql updated! Entries: " + result.size());
            } else {
                log.warn("You must init the DB connection first!");
                prepare(config);
            }
        } catch (SQLException e) {
            log.error("The postgreSQL query failed! " + e.toString());
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) rs.close();
            } catch (Exception e) {
            }
            try {
                if (st != null) st.close();
            } catch (Exception e) {
            }
        }

        return result;
    }
}
