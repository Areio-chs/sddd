package com.atguigu.zk.test;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

public class ZkTest {
	
	private ZooKeeper zooKeeper;
	//代码块初始化，每一个实例都会执行
	{
		
		String connectString = "192.168.80.128:2181";
		int sessionTimeout = 5000;
		Watcher watcher = new Watcher() {
			@Override
			public void process(WatchedEvent event) {}
		};
		
		try {
			zooKeeper = new ZooKeeper(connectString, sessionTimeout, watcher);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testUpdateNodeData() throws KeeperException, InterruptedException {
		
		// 要操作的节点的路径
		String path = "/animal/cat";
		
		// 获取节点当前值
		byte[] resultByteArray = zooKeeper.getData(path, false, new Stat());
		
		// 将字节数组封装为字符串
		String result = new String(resultByteArray);
		
		// 打印旧值
		System.out.println("old value="+result);
		
		// 获取新值字符串对应的字节数组
		byte[] newValueByteArray = new String("miaomiao").getBytes();
		
		// 指定当前操作所基于的版本号，如果不确定可以使用-1
		int version = -1;
		
		// 执行节点值的修改，返回结点状态
		Stat stat = zooKeeper.setData(path, newValueByteArray, version);
		
		// 获取最新版本号
		int newVersion = stat.getVersion();
		
		System.out.println("newVersion="+newVersion);
		
		// 获取节点新值
		resultByteArray = zooKeeper.getData(path, false, new Stat());
		
		result = new String(resultByteArray);
		
		System.out.println("new value="+result);
	}

	@Test
	public void testNoticeOnce() throws KeeperException, InterruptedException {

		String path = "/animal/cat";
		//接口，实现匿名内部类
		Watcher watcher = new Watcher() {

			@Override
			// 当前Watcher检测到节点值的修改，会调用这个process()方法
			public void process(WatchedEvent event) {
				System.err.println("接收到了通知！值发生了修改！");
			}
		};

		byte[] oldValue = zooKeeper.getData(path, watcher, new Stat());

		System.out.println("old value="+new String(oldValue));

		while(true) {
			Thread.sleep(5000);
			System.err.println("当前方法原本要执行的业务逻辑");
		}
	}

	@Test
	public void testNoticeForever() throws KeeperException, InterruptedException {

		String path = "/animal/cat";

		getDataWithNotice(zooKeeper, path);
       //持续运行这个方法体不要停，睡5秒才打印一次
		while(true) {
			Thread.sleep(5000);
			System.err.println("当前方法原本要执行的业务逻辑 线程名称："+Thread.currentThread().getName());
		}
	}



	public void getDataWithNotice(ZooKeeper zooKeeper, String path) throws KeeperException, InterruptedException {

		byte[] resultByteArray = zooKeeper.getData(path, new Watcher() {

			@Override
			public void process(WatchedEvent event) {

				// 以类似递归的方式调用getDataWithNotice()方法实现持续监控
				try {
					getDataWithNotice(zooKeeper, path);
					System.err.println("通知 线程名称："+Thread.currentThread().getName());
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
		}, new Stat());

		String result = new String(resultByteArray);

		System.err.println("当前节点值="+result);
	}
}
