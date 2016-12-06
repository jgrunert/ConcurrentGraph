package mthesis.concurrent_graph.communication;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
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
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import mthesis.concurrent_graph.Settings;
import mthesis.concurrent_graph.node.AbstractNode;
import mthesis.concurrent_graph.util.Pair;


/**
 * Class to handle messaging between nodes.
 * Based on Netty channels.
 * 
 * @author jonas
 *
 */
public class MessageSenderAndReceiver {
	private final Logger logger;

	private final int ownId;
	private final Map<Integer, Pair<String, Integer>> machines;
	private final ConcurrentHashMap<Integer, Channel> activeChannels = new ConcurrentHashMap<Integer, Channel>();

	private EventLoopGroup bossGroup;
	private EventLoopGroup workerGroup;

	private final AbstractNode messageListener;


	public MessageSenderAndReceiver(Map<Integer, Pair<String, Integer>> machines, int ownId,
			AbstractNode listener) {
		this.logger = LoggerFactory.getLogger(this.getClass() + "[" + ownId + "]");
		this.ownId = ownId;
		this.machines = machines;
		this.messageListener = listener;
	}

	//	public void setMessageListner(AbstractNode listener) {
	//		this.messageListener = listener;
	//	}


	public void start() {
		bossGroup = new NioEventLoopGroup(1);
		workerGroup = new NioEventLoopGroup();

		startServer();

		// Connect to all other machines with smaller IDs
		for(final Entry<Integer, Pair<String, Integer>> machine : machines.entrySet()) {
			if(machine.getKey() < ownId) {
				try {
					connectToMachine(machine.getValue().fst, machine.getValue().snd);
				} catch (final Exception e) {
					logger.error("Exception at connectToMachine " + machine.getKey(), e);
				}
			}
		}
	}

	public boolean waitUntilConnected() {
		final long timeoutTime = System.currentTimeMillis() + Settings.CONNECT_TIMEOUT;
		while(System.currentTimeMillis() <= timeoutTime && activeChannels.size() < (machines.size() - 1)) {
			Thread.yield();
		}
		if(activeChannels.size() == (machines.size() - 1)) {
			logger.info("Established all connections");
			return true;
		}
		else {
			logger.error("Timeout while wait for establish all connections");
			return false;
		}
	}

	public void stop() {
		bossGroup.shutdownGracefully();
		workerGroup.shutdownGracefully();
	}


	public void sendMessageTo(int machineId, String message) {
		// TODO Checks
		activeChannels.get(machineId).writeAndFlush(message + "\n");
	}

	public void sendMessageTo(List<Integer> machineIds, String message) {
		// TODO Checks
		for(final Integer machineId : machineIds) {
			activeChannels.get(machineId).writeAndFlush(message + "\n");
		}
	}


	public void onIncomingMessage(String message) {
		if(messageListener != null)
			messageListener.onIncomingMessage(message);
	}


	private void connectToMachine(String host, int port) throws Exception {
		// Configure SSL.git
		final SslContext sslCtx;
		if (Settings.SSL) {
			sslCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
		} else {
			sslCtx = null;
		}

		// Configure the client.
		final Bootstrap b = new Bootstrap();
		b.group(workerGroup)
		.channel(NioSocketChannel.class)
		.option(ChannelOption.TCP_NODELAY, Settings.TCP_NODELAY)
		.option(ChannelOption.SO_KEEPALIVE, Settings.KEEPALIVE)
		.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
				final ChannelPipeline p = ch.pipeline();
				if (sslCtx != null) {
					p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
				}
				// p.addLast(new LoggingHandler(LogLevel.INFO));
				p.addLast(new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
				p.addLast(new StringEncoder());
				p.addLast(new StringDecoder());
				p.addLast(new MachineChannelHandler(activeChannels, ownId, MessageSenderAndReceiver.this));
			}
		});

		// Start the client.
		b.connect(host, port).channel();
	}


	private void startServer() {
		final Thread serverThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					logger.info("Start run connection server");
					runServer();
					logger.info("End run connection server");
				} catch (final Exception e) {
					logger.error("Exception at runServer", e);
				}
			}
		});
		serverThread.setName("ServerThread-Machine" + ownId);
		serverThread.setDaemon(true);
		serverThread.start();
	}

	private void runServer() throws Exception {
		final int port = machines.get(ownId).snd;

		// Configure SSL.
		final SslContext sslCtx;
		if (Settings.SSL) {
			final SelfSignedCertificate ssc = new SelfSignedCertificate();
			sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
		} else {
			sslCtx = null;
		}

		final ServerBootstrap b = new ServerBootstrap();
		b.group(bossGroup, workerGroup)
		.channel(NioServerSocketChannel.class)
		.option(ChannelOption.SO_BACKLOG, 100)
		.option(ChannelOption.SO_KEEPALIVE, Settings.KEEPALIVE)
		.option(ChannelOption.TCP_NODELAY, Settings.TCP_NODELAY).handler(new LoggingHandler(LogLevel.INFO))
		.childHandler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
				final ChannelPipeline p = ch.pipeline();
				if (sslCtx != null) {
					p.addLast(sslCtx.newHandler(ch.alloc()));
				}
				// p.addLast(new LoggingHandler(LogLevel.INFO));
				p.addLast(new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
				p.addLast(new StringEncoder());
				p.addLast(new StringDecoder());
				p.addLast(new MachineChannelHandler(activeChannels, ownId, MessageSenderAndReceiver.this));
			}
		});

		// Start the server.
		final ChannelFuture f = b.bind(port).sync();

		// Wait until the server socket is closed.
		f.channel().closeFuture().sync();
	}
}
