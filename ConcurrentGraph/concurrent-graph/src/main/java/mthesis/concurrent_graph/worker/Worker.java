package mthesis.concurrent_graph.worker;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import mthesis.concurrent_graph.Settings;


/**
 * Concurrent graph processing worker main
 */
public class Worker {
	private final String host;
	private final int port;
	
	public Worker(String host, int port) {
		super();
		this.host = host;
		this.port = port;
	}
	
	public void run() {
//		EventLoopGroup group = new NioEventLoopGroup();
//		
//		try {
//			Bootstrap bootstrap = new Bootstrap()
//					.group(group)
//					.channel(NioSocketChannel.class)
//					.handler(new WorkerChannelInitializer());
//			
//			Channel channel = bootstrap.connect(host, port).sync().channel();
//			channel.write("abc");
//		}
//		finally {
//			group.shutdownGracefully();
//		}
		
		EventLoopGroup workerGroup = new NioEventLoopGroup(); // (2)
		try {
			Bootstrap bootstrap = new Bootstrap(); // (3)
			bootstrap.group(workerGroup); // (4)
			bootstrap.channel(NioSocketChannel.class); // (5)
			bootstrap.option(ChannelOption.SO_KEEPALIVE, Settings.KEEPALIVE); // (6)
			bootstrap.handler(new ChannelInitializer<SocketChannel>() { // (7)
				@Override
				protected void initChannel(SocketChannel channel) throws Exception {
					System.out.println("Verbindung zum Server hergestellt!");
				}
			});
			bootstrap.connect(host, port).sync().channel().closeFuture().sync(); // (8)
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			workerGroup.shutdownGracefully(); // (9)
		}
	}
	
	public static void main(String[] args) {
		System.out.println("Worker starting");
		try {
			new Worker("localhost", 1234).run();			
		} catch(Exception exc) {
			exc.printStackTrace();
		}
		System.out.println("Worker end");
	}
}
