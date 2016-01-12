package net.redborder.samza.stores;

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
            "namespace", "market", "organization", "service_provider", "zone", "campus_uuid",
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
    }

    @Override
    public Map<String, Map<String, Object>> update() {
        Statement st = null;
        ResultSet rs = null;
        long entries = 0L;
        Map<String, Map<String, Object>> result = null;
        try {
            if (conn != null) {
                st = conn.createStatement();
                rs = st.executeQuery("QUERY");
                ObjectMapper mapper = new ObjectMapper();

                Map<String, Map<String, Object>> tmpCache = new HashMap<>();

                while (rs.next()) {
                    Map<String, Object> location = new HashMap<>();
                    Map<String, String> enriching = new HashMap<>();

                    String enrichmentStr = rs.getString("enrichment");
                    if (enrichmentStr != null) {
                        try {
                            Map<String, String> enrichment = mapper.readValue(enrichmentStr, Map.class);
                            enriching.putAll(enrichment);
                        } catch (JsonMappingException e) {
                            e.printStackTrace();
                        } catch (JsonParseException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    String longitude = rs.getString("longitude");
                    String latitude = rs.getString("latitude");

                    for (String columnName : enrichColumns) {
                        String columnData = rs.getString(columnName);
                        if (columnData != null) enriching.put(columnName, columnData);
                    }

                    entries++;

                    if (longitude != null && latitude != null) {
                        Double longitudeDbl = (double) Math.round(Double.valueOf(longitude) * 100000) / 100000;
                        Double latitudeDbl = (double) Math.round(Double.valueOf(latitude) * 100000) / 100000;
                        location.put("client_latlong", latitudeDbl + "," + longitudeDbl);
                    }

                    location.putAll(enriching);

                    result = new HashMap<>();
                    if (!location.isEmpty()) {
                        log.debug("IOT: {} LOCATION: {}", rs.getString("sensor_uuid"), location);

                        String sensorUUID = rs.getString("sensor_uuid");

                        if (sensorUUID != null) {
                            tmpCache.put(sensorUUID, location);
                            result.put(sensorUUID, location);
                        }

                        String proxyUUID = rs.getString("proxy_uuid");

                        if (proxyUUID != null) {
                            tmpCache.put(proxyUUID, location);
                            result.put(proxyUUID, location);
                        }
                    }
                }

                log.info("PostgreSql updated! Entries: " + entries);
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
