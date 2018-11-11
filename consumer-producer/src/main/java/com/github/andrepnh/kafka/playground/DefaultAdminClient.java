package com.github.andrepnh.kafka.playground;

import static com.github.andrepnh.kafka.playground.ClusterProperties.BOOTSTRAP_SERVERS;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;

public final class DefaultAdminClient {
  public static final AdminClient INSTANCE = AdminClient.create(
      ImmutableMap.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS));

  private DefaultAdminClient() { }
}
