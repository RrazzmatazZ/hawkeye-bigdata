package com.dreamtail.observer;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author xdq
 * @version 1.0
 * @className ESClient
 * @description TODO
 * @date 2021/5/11 15:30
 */
public class ESClient {

    public static Client client;

    /**
     * init ES client
     */
    public static void initEsClient() throws UnknownHostException {
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        Settings esSettings = Settings.builder().put("cluster.name", "elasticsearch").build();
        client = new PreBuiltTransportClient(esSettings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("hadoop249"), 9300));

    }

    /**
     * Close ES client
     */
    public static void closeEsClient() {
        client.close();
    }

    public static void main(String[] args) throws UnknownHostException {
        initEsClient();
        System.out.println(client.hashCode());
    }
}