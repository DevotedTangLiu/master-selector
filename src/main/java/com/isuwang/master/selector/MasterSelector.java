package com.isuwang.master.selector;

import com.isuwang.dapeng.core.SoaSystemEnvProperties;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tangliu on 2016/7/12.
 */
public class MasterSelector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MasterSelector.class);

    public static Map<String, String> masterServices = new HashMap<String, String>();

    /**
     * 根据serviceName，versionName，host， port判断是否为master
     *
     * @param servieName
     * @param versionName
     * @param host
     * @param port
     * @return
     */
    public static boolean isMaster(String servieName, String versionName, String host, Integer port) {

        String key = generateKey(servieName, versionName);
        String address = generateKey(host, String.valueOf(port));

        if (!masterServices.containsKey(key)) {
            return true;//// TODO: 2016/7/12 true or false?
        } else {
            if (address.equals(masterServices.get(key)))
                return true;
            else
                return false;
        }
    }

    private static final String PATH = "/soa/master/services/";

    private ZooKeeper zk;

    public String zookeeperHost = SoaSystemEnvProperties.SOA_ZOOKEEPER_HOST;

    public String getZookeeperHost() {
        return zookeeperHost;
    }

    public void setZookeeperHost(String zookeeperHost) {
        this.zookeeperHost = zookeeperHost;
    }


    public MasterSelector() {
        init();
    }

    public void init() {

        try {
            zk = new ZooKeeper(zookeeperHost, 15000, watchedEvent -> {
                if (watchedEvent.getState() == Watcher.Event.KeeperState.Expired) {
                    LOGGER.info("MasterSelector {} Session过期,重连 [Zookeeper]", zookeeperHost);
                    destroy();
                    init();
                } else if (Watcher.Event.KeeperState.SyncConnected == watchedEvent.getState()) {
                    LOGGER.info("MasterSelector {} [Zookeeper]", zookeeperHost);
                    addMasterRoute();
                    getMasterServices();
                }
            });
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public void destroy() {
        if (zk != null) {
            try {
                zk.close();
                zk = null;
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }

    }


    private void getMasterServices() {

        zk.getChildren("/soa/master/services", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //Children发生变化，则重新获取最新的services列表
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    LOGGER.info("{}子节点发生变化，重新获取子节点...", event.getPath());
                    getMasterServices();
                }
            }
        }, (rc, path, ctx, children) -> {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getMasterServices();
                    break;
                case OK:
                    LOGGER.info("获取Master Services列表成功");
                    getMasterServiceInfo(path, children);
                    break;
                default:
                    LOGGER.error("get master services fail");
            }
        }, null);
    }


    private void getMasterServiceInfo(String path, List<String> list) {

        for (String serviceKey : list) {
            getServiceInfoByPath(path + "/" + serviceKey, serviceKey);
        }
    }

    /**
     * 根据serviceKey获取内容
     *
     * @param servicePath
     */
    private void getServiceInfoByPath(String servicePath, String serviceKey) {

        zk.getData(servicePath, event -> {
            if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                LOGGER.info(event.getPath() + "'s data changed, reset master service in memory");
                String[] paths = event.getPath().split("/");
                getServiceInfoByPath(event.getPath(), paths[paths.length - 1]);
            }
        }, (rc, path1, ctx, data, stat) -> {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getServiceInfoByPath(path1, (String) ctx);
                    break;
                case OK:
                    String address = new String(data);
                    masterServices.put((String) ctx, address);
                    break;
                default:
                    LOGGER.error("Error when trying to get data of {}.", path1);
            }
        }, serviceKey);
    }


    public static String generateKey(String serviceName, String versionName) {
        return serviceName + ":" + versionName;
    }

    /**
     * 竞选Master
     * <p>
     * /soa/master/services/**.**.**.AccountService:1.0.0   data [192.168.99.100:9090]
     */
    public void runForMaster(String key) {
        runForMaster(key, currentContainerAddr);
    }

    /**
     * @param key
     * @param value
     */
    public void runForMaster(String key, String value) {
        zk.create(PATH + key, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCb, key);
    }


    private AsyncCallback.StringCallback masterCreateCb = (rc, path, ctx, name) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                //检查master状态
                checkMaster((String) ctx);
                break;
            case OK:
                //被选为master
                LOGGER.info("{}竞选master成功", (String) ctx);
                break;
            case NODEEXISTS:
                //master节点上已存在相同的service:version，自己没选上
//                isMaster.put((String) ctx, false);
                LOGGER.info("{}竞选master失败", (String) ctx);
                //保持监听
                masterExists((String) ctx);
                break;
            case NONODE:
                LOGGER.error("{}的父节点不存在，创建失败", path);
                break;
            default:
                LOGGER.error("创建{}异常：{}", path, KeeperException.Code.get(rc));
        }
    };

    /**
     * 监听master是否存在
     */
    private void masterExists(String key) {

        zk.exists(PATH + key, event -> {
            //若master节点已被删除,则竞争master
            if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                String serviceKey = event.getPath().replace(PATH, "");
                runForMaster(serviceKey);
            }

        }, (rc, path, ctx, stat) -> {

            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    masterExists((String) ctx);
                    break;
                case NONODE:
                    runForMaster((String) ctx);
                    break;
                case OK:
                    if (stat == null) {
                        runForMaster((String) ctx);
                    } else {
                        checkMaster((String) ctx);
                    }
                    break;
                default:
                    checkMaster((String) ctx);
                    break;
            }

        }, key);
    }

    /**
     * 检查master
     *
     * @param serviceKey
     */
    private void checkMaster(String serviceKey) {

        zk.getData(PATH + serviceKey, false, (rc, path, ctx, data, stat) -> {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster((String) ctx);
                    return;
                case NONODE: // 没有master节点存在，则尝试获取领导权
                    runForMaster((String) ctx);
                    return;
                case OK:
                    String value = new String(data);
                    masterServices.put((String) ctx, value);
                    return;
            }

        }, serviceKey);
    }


    public final String currentContainerAddr = SoaSystemEnvProperties.SOA_CONTAINER_IP + ":" + String.valueOf(SoaSystemEnvProperties.SOA_CONTAINER_PORT);

    /**
     * 创建/soa/master/services节点
     */
    private void addMasterRoute() {
        String[] paths = PATH.split("/");
        String route = "/";
        for (int i = 1; i < paths.length; i++) {
            route += paths[i];
            addPersistServerNode(route, "");
            route += "/";
        }
    }

    /**
     * 添加持久化的节点
     *
     * @param path
     * @param data
     */
    private void addPersistServerNode(String path, String data) {
        Stat stat = exists(path);

        if (stat == null)
            zk.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, persistNodeCreateCb, data);
    }

    private Stat exists(String path) {
        Stat stat = null;
        try {
            stat = zk.exists(path, false);
        } catch (KeeperException e) {
        } catch (InterruptedException e) {
        }
        return stat;
    }

    /**
     * 添加持久化节点回调方法
     */
    private AsyncCallback.StringCallback persistNodeCreateCb = (rc, path, ctx, name) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                LOGGER.info("创建节点:{},连接断开，重新创建", path);
                addPersistServerNode(path, (String) ctx);
                break;
            case OK:
                LOGGER.info("创建节点:{},成功", path);
                break;
            case NODEEXISTS:
                LOGGER.info("创建节点:{},已存在", path);
                break;
            default:
                LOGGER.info("创建节点:{},失败", path);
        }
    };


    public static void main(String[] args) {

        MasterSelector ms = new MasterSelector();
        ms.runForMaster("helloService");

        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(masterServices);
    }
}
