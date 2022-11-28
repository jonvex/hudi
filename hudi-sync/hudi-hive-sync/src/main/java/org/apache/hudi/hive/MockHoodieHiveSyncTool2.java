package org.apache.hudi.hive;

import org.apache.hudi.sync.common.HoodieSyncTool;

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

public class MockHoodieHiveSyncTool2 extends HoodieSyncTool {

  public static boolean syncSuccess;

  public MockHoodieHiveSyncTool2(Properties props, Configuration hadoopConf) {
    super(props, hadoopConf);
  }

  @Override
  public void syncHoodieTable() {
    syncSuccess = true;
  }
}

