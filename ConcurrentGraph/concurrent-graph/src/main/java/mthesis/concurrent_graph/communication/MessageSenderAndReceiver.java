package mthesis.concurrent_graph.communication;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import mthesis.concurrent_graph.Pair;
import mthesis.concurrent_graph.Settings;
import mthesis.concurrent_graph.playground.EchoServerHandler;


public class MessageSenderAndReceiver {
	private static Logger logger = LoggerFactory.getLogger(MessageSenderAndReceiver.class);

	private final int ownId;
	private final Map<Integer, Pair<String, Integer>> machines;
	private ConcurrentHashMap<Integer, Channel> activeChannels = new ConcurrentHashMap<Integer, Channel>();

	
	public MessageSenderAndReceiver(Map<Integer, Pair<String, Integer>> machines, int ownId) {
		this.ownId = ownId;
		this.machines = machines;
	}
	
	
	public void start() {
		startServer();
	}
	
	
	private void startServer() {
		Thread serverThread = new Thread(new Runnable() {			
			@Override
			public void run() {
				try {
					runServer();
				} catch (Exception e) {
					logger.error("Exception at runServer", e);
				}
			}
		});
		serverThread.setName("ServerThread-Machine" + ownId);
		serverThread.setDaemon(true);
		serverThread.start();
	}
	
	private void runServer() throws Exception {
		int port = machines.get(ownId).snd;	
		
        // Configure SSL.
        final SslContext sslCtx;
        if (Settings.SSL) {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
        } else {
            sslCtx = null;
        }

        // Configure the server.
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .option(ChannelOption.SO_BACKLOG, 100)
             .handler(new LoggingHandler(LogLevel.INFO))
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception {
                     ChannelPipeline p = ch.pipeline();
                     if (sslCtx != null) {
                         p.addLast(sslCtx.newHandler(ch.alloc()));
                     }
                     p.addLast(new MachineChannelHandler(activeChannels));
                 }
             });

            // Start the server.
            ChannelFuture f = b.bind(port).sync();

            // Wait until the server socket is closed.
            f.channel().closeFuture().sync();
        } finally {
            // Shut down all event loops to terminate all threads.
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
