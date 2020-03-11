package org.tyshawn.lock.zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperConnection {
    private ZooKeeper zoo;
    private final CountDownLatch connectedSignal = new CountDownLatch(1);

    /**
     * 连接ZooKeeper
     * @param host
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public ZooKeeper connect(String host) throws IOException, InterruptedException {
        zoo = new ZooKeeper(host, 5000, new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });
        //因为监听器是异步操作, 要保证监听器操作先完成, 即要确保先连接上ZooKeeper再返回实例.
        connectedSignal.await();
        return zoo;
    }

    /**
     * 关闭连接
     * @throws InterruptedException
     */
    public void close() throws InterruptedException {
        zoo.close();
    }

}
