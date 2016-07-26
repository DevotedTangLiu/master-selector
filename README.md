### 服务使用Zookeeper竞选Master

#### 背景

一个服务可能存在一些定时任务，比如每分钟去操作一下数据库之类的。当系统只有一个容器时，不用考虑主从的问题。但如果系统是分布式的，一个服务可能同时运行在多个容器中，某些查询类的定时任务没有影响，但是某些定时任务每次只需要执行一次，没有区分主从的情况下，每个容器下的服务都会企图去执行，可能会造成不可预料的结果。

所以，我们需要服务能够识别自己是否是Master，如果是，则执行，如果不是，则跳过。同时，如果Master服务掉线，那么其他容器里的slave服务能够自动升级为Master，并执行Master执行的任务。

#### 基础

* Zookeeper客户端可以创建临时节点并保持长连接，当客户端断开连接时，临时节点会被删除
* Zookeeper客户端可以监听节点变化

#### 实现

1. 定义一个持久化节点`/soa/master/services`，此节点下的子节点为临时节点，分别代表不同的Master服务
2. Container_1中的服务AccountService，在启动时，在zookeeper中创建临时节点`/soa/master/services/AccountService:1.0.0`,节点的数据为`192.168.99.100:9090`。这代表，`192.168.99.100:9090`这个容器中的AccountService(版本为1.0.0)成功竞选为Master服务。Container_1中维护一个缓存，如果竞选成功，对应service:version置为true，否则置为false;
3. Container_2中的服务AccountService，在启动时，也试图创建临时节点`/soa/master/services/AccountService:1.0.0`,但是会创建失败，返回结果码显示该节点已经存在。所以服务就知道已经有一个Master的AccountService(1.0.0)存在，它竞选失败。
4. Container_2会保持对该临时节点的监听,如果监听到该零时节点被删除，则试图再次创建(创建临时节点的过程就是竞选master的过程)，创建成功，则更新缓存对应service:version为true，否则继续保持监听。

#### 优化

不管竞选成功还是失败，可以维护一份Master缓存信息,并保持监听，实时更新。这样，不仅能够自动竞选master，还能够通过修改临时节点数据的方式，手动指定Master。

#### 关键代码

```
/**
 * 竞选Master
 * <p/>
 * /soa/master/services/**.**.**.AccountService:1.0.0   data [192.168.99.100:9090]
 */
public void runForMaster(String key) {
    zk.create(PATH + key, currentContainerAddr.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCb, key);
}

private AsyncCallback.StringCallback masterCreateCb = (rc, path, ctx, name) -> {
    switch (KeeperException.Code.get(rc)) {
        case CONNECTIONLOSS:
            //检查master状态
            checkMaster((String) ctx);
            break;
        case OK:
            //被选为master
            isMaster.put((String) ctx, true);
            LOGGER.info("{}竞选master成功, data为[{}]", (String) ctx, currentContainerAddr);
            break;
        case NODEEXISTS:
            //master节点上已存在相同的service:version，自己没选上
            isMaster.put((String) ctx, false);
            LOGGER.info("{}竞选master失败, data为[{}]", (String) ctx, currentContainerAddr);
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
                if (value.equals(currentContainerAddr))
                    isMaster.put((String) ctx, true);
                else
                    isMaster.put((String) ctx, false);
                return;
        }

    }, serviceKey);
}
```
