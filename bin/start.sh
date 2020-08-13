sh /root/maoxiangyi/flink_integration_hive/bin/compile.sh
#kinit -kt /etc/security/keytabs/dp_hive.keytab dp_hive
cd /root/maoxiangyi
/export/servers/nc/flink/bin/flink run -c com.tal.flink.hive.StreamMain /root/maoxiangyi/flink_integration_hive-1.0-SNAPSHOT.jar

