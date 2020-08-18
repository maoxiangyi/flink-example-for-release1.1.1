package com.mxy.streaming;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

public class FlinkStream2Hive {
    public static void main(String[] args) throws Exception {
        //创建Streaming model
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置checkpoint
        env.enableCheckpointing(10000);   //设置checkpoint的相关参数
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setCheckpointTimeout(15000);

        //添加source
        DataStreamSource<String> streamSource = env.addSource(new MySource());

        //设置写入到hive的配置信息
        final StreamingFileSink<String> sink = StreamingFileSink
                //forRowFormat 表示输出的文件是按行存储的，对应的还有 forBulkFormat，可以将输出结果用 Parquet 等格式进行压缩存储。
                .forRowFormat(new Path("hdfs://ns1/warehouse/fs_table/"), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH-mm")) //先设置分桶
                .withRollingPolicy(  //设置每个桶中小文件滚动的策略
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024 * 1024 * 1024)  // 设置每个文件的最大大小 ,默认是128M。
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(60)) // 滚动写入新文件的时间，默认60s。这里设置为60
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)) // 60s空闲，就滚动写入新的文件
                                //间隔检查时间， 是以系统时间0秒时刻开始算的。
                                // 如10:00:10运行程序，inactivityInterval=300s，bucketCheckInterval=60s，最后一次写入时间是10:05:30。
                                // 则在10:11:00时，将inprocess文件转为正式文件。
                                .build())
                .withBucketCheckInterval(1000L) //桶间隔检测时间，设置为1S
                .build();

        //将sink添加到source中
        streamSource.addSink(sink);
        //执行作业
        env.execute();
    }


    public static class MySource implements SourceFunction<String> {

        String userids[] = {
                "4760858d-2bec-483c-a535-291de04b2247", "67088699-d4f4-43f2-913c-481bff8a2dc5",
                "72f7b6a8-e1a9-49b4-9a0b-770c41e01bfb", "dfa27cb6-bd94-4bc0-a90b-f7beeb9faa8b",
                "aabbaa50-72f4-495c-b3a1-70383ee9d6a4", "3218bbb9-5874-4d37-a82d-3e35e52d1702",
                "3ebfb9602ac07779||3ebfe9612a007979", "aec20d52-c2eb-4436-b121-c29ad4097f6c",
                "e7e896cd939685d7||e7e8e6c1930689d7", "a4b1e1db-55ef-4d9d-b9d2-18393c5f59ee"
        };

        @Override
        public void run(SourceFunction.SourceContext<String> sourceContext) throws Exception {

            while (true) {
                String userid = userids[(int) (Math.random() * (userids.length - 1))];
                UserInfo userInfo = new UserInfo();
                userInfo.setUserId(userid);
                userInfo.setAmount(Math.random() * 100);
                userInfo.setTs(new Timestamp(System.currentTimeMillis()));
                sourceContext.collect(userInfo.format());
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {

        }
    }

    public static class UserInfo implements java.io.Serializable {
        private String userId;
        private Double amount;
        private Timestamp ts;

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public Double getAmount() {
            return amount;
        }

        public void setAmount(Double amount) {
            this.amount = amount;
        }

        public Timestamp getTs() {
            return ts;
        }

        public void setTs(Timestamp ts) {
            this.ts = ts;
        }

        @Override
        public String toString() {
            return "UserInfo{" +
                    "userId='" + userId + '\'' +
                    ", amount=" + amount +
                    ", ts=" + ts +
                    '}';
        }

        public String format() {
            return userId + "\r\n" + amount + "\r\n" + ts;
        }
    }
}
