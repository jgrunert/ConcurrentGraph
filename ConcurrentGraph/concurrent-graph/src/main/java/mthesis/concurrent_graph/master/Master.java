package mthesis.concurrent_graph.master;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import mthesis.concurrent_graph.Settings;

/**
 * Concurrent graph processing master main
 */
public class Master {
	
	
	public void run() {
		EventLoopGroup bossGroup = new NioEventLoopGroup(); // (1)
		EventLoopGroup workerGroup = new NioEventLoopGroup(); // (1)
		try {
			ServerBootstrap bootstrap = new ServerBootstrap(); // (2)
			bootstrap.group(bossGroup, workerGroup); // (3)
			bootstrap.channel(NioServerSocketChannel.class); // (4)
			bootstrap.childHandler(new ChannelInitializer<SocketChannel>() { // 5
				@Override
				protected void initChannel(SocketChannel channel) throws Exception {
					System.out.println("Ein Computer hat sich verbunden. IP: " + channel.remoteAddress().getHostName()); // (6)
				}
			});
			bootstrap.option(ChannelOption.SO_BACKLOG, 50); // (7)
			bootstrap.childOption(ChannelOption.SO_KEEPALIVE, Settings.KEEPALIVE); // (8)
			ChannelFuture future = bootstrap.bind(Settings.PORT).sync(); // (9)
			System.out.println("Server gestartet!");
			future.channel().closeFuture().sync(); // (10)
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			bossGroup.shutdownGracefully(); // (11)
			workerGroup.shutdownGracefully(); // (11)
		}
	}
	
	
	public static void main(String[] args) {
		System.out.println("Master starting");
		try {
			new Master().run();			
		} catch(Exception exc) {
			exc.printStackTrace();
		}
	}
}
