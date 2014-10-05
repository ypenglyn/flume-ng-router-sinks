package com.datums.stream;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroRouterSink extends AbstractPluralRpcSink {

  private static final Logger logger = LoggerFactory.getLogger(AvroRouterSink.class);

  @Override
  protected RpcClient initializeRpcClient(Properties props) {
    logger.info("Attempting to create Avro Plural Rpc main client.");
    return RpcClientFactory.getInstance(props);
  }

  @Override
  protected Map<Pattern, RpcClient> initializePluralRpcClient(Map<Pattern, Properties> propList) {
    logger.info("Attempting to create Avro Plural Rpc plural client.");
    Map<Pattern, RpcClient> rpcClients = new HashMap<>();
    for (Entry<Pattern, Properties> entry : propList.entrySet()) {
      rpcClients.put(entry.getKey(), RpcClientFactory.getInstance(entry.getValue()));
    }
    return rpcClients;
  }

}
