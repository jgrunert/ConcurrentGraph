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
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import mthesis.concurrent_graph.Settings;
import mthesis.concurrent_graph.communication.Messages.ControlMessage;
import mthesis.concurrent_graph.communication.Messages.VertexMessage;
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
		this.logger = LoggerFactory.getLogger(this.getClass().getCanonicalName() + "[" + ownId + "]");
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

		try {
			startServer();
		}
		catch (final Exception e2) {
			logger.error("Starting server failed", e2);
		}

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



	int msgvs = 0;
	public void sendVertexMessage(int machineId, VertexMessage message) {
		// TODO Checks
		final Channel ch = activeChannels.get(machineId);
		ch.write(message);
	}
	public void sendVertexMessage(List<Integer> machineIds, VertexMessage message) {
		// TODO Checks
		for(final Integer machineId : machineIds) {
			sendVertexMessage(machineId, message);// TODO Re-use message
		}
	}

	public void sendControlMessage(int dstId, Messages.ControlMessage message, boolean flush) {
		// TODO Checks
		final Channel ch = activeChannels.get(dstId);
		if(flush)
			ch.writeAndFlush(message);
		else
			ch.write(message);
	}
	public void sendControlMessage(List<Integer> dstIds, Messages.ControlMessage message, boolean flush) {
		// TODO Checks
		for(final Integer machineId : dstIds) {
			sendControlMessage(machineId, message, flush);// TODO Re-use message
		}
	}

	public void onIncomingControlMessage(ControlMessage message) {
		messageListener.onIncomingControlMessage(message);
	}

	public void onIncomingVertexMessage(VertexMessage message) {
		messageListener.onIncomingVertexMessage(message);
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
		.option(ChannelOption.SO_KEEPALIVE, Settings.KEEPALIVE)
		.option(ChannelOption.TCP_NODELAY, Settings.TCP_NODELAY)
		//.option(ChannelOption.SO_RCVBUF, 2048)
		.handler(new LoggingHandler(LogLevel.INFO))
		.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
				final ChannelPipeline p = ch.pipeline();
				if (sslCtx != null) {
					p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
				}
				// p.addLast(new LoggingHandler(LogLevel.INFO));
				//				p.addLast(new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
				//				p.addLast(new StringEncoder());
				//				p.addLast(new StringDecoder());
				//p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(1048576, 0, 2, 0, 2));
				p.addLast(new ProtobufVarint32FrameDecoder());
				p.addLast(new ProtobufDecoder(Messages.ControlMessage.getDefaultInstance()));
				p.addLast(new ProtobufDecoder(Messages.VertexMessage.getDefaultInstance()));
				p.addLast(new ProtobufVarint32LengthFieldPrepender());
				p.addLast(new ProtobufEncoder());
				p.addLast(new ControlMessageHandler(activeChannels, ownId, MessageSenderAndReceiver.this));
				p.addLast(new VertexMessageHandler(ownId, MessageSenderAndReceiver.this));
			}
		});

		// Start the client.
		b.connect(host, port).channel();
	}


	private void startServer() throws Exception {
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
		.option(ChannelOption.TCP_NODELAY, Settings.TCP_NODELAY)
		//.option(ChannelOption.SO_RCVBUF, 2048)
		.handler(new LoggingHandler(LogLevel.INFO))
		.childHandler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
				final ChannelPipeline p = ch.pipeline();
				if (sslCtx != null) {
					p.addLast(sslCtx.newHandler(ch.alloc()));
				}
				// p.addLast(new LoggingHandler(LogLevel.INFO));
				//				p.addLast(new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
				//				p.addLast(new StringEncoder());
				//				p.addLast(new StringDecoder());
				// TODO maxFrameLength config
				//p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(1048576, 0, 2, 0, 2));
				p.addLast(new ProtobufVarint32FrameDecoder());
				p.addLast(new ProtobufDecoder(Messages.ControlMessage.getDefaultInstance()));
				p.addLast(new ProtobufDecoder(Messages.VertexMessage.getDefaultInstance()));
				p.addLast(new ProtobufVarint32LengthFieldPrepender());
				p.addLast(new ProtobufEncoder());
				p.addLast(new ControlMessageHandler(activeChannels, ownId, MessageSenderAndReceiver.this));
				p.addLast(new VertexMessageHandler(ownId, MessageSenderAndReceiver.this));
			}
		});

		// Start the server.
		b.bind(port).sync();
		logger.info("Started connection server");
	}
}
