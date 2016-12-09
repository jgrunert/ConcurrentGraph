package mthesis.concurrent_graph.communication;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
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
		final ByteBuf outBuf = ch.alloc().buffer(6*4);
		outBuf.writeInt(MessageType.Vertex.ordinal());
		outBuf.writeInt(message.SuperstepNo);
		outBuf.writeInt(message.FromNode);
		outBuf.writeInt(message.FromVertex);
		outBuf.writeInt(message.ToVertex);
		outBuf.writeInt(message.Content);
		ch.write(outBuf);
		//logger.debug("S Vertex message: " + msgvs++);
	}
	public void sendVertexMessage(List<Integer> machineIds, VertexMessage message) {
		// TODO Checks
		for(final Integer machineId : machineIds) {
			sendVertexMessage(machineId, message);// TODO Reu se buffer
		}
	}

	public void sendControlMessage(int machineId, ControlMessage message, boolean flush) {
		// TODO Checks
		final Channel ch = activeChannels.get(machineId);;
		final ByteBuf outBuf = ch.alloc().buffer(5*4);
		outBuf.writeInt(message.Type.ordinal());
		outBuf.writeInt(message.SuperstepNo);
		outBuf.writeInt(message.FromNode);
		outBuf.writeInt(message.Content1);
		outBuf.writeInt(message.Content2);
		if(flush)
			ch.writeAndFlush(outBuf);
		else
			ch.write(outBuf);
	}
	public void sendControlMessage(List<Integer> machineIds, ControlMessage message, boolean flush) {
		// TODO Checks
		for(final Integer machineId : machineIds) {
			sendControlMessage(machineId, message, flush);// TODO Reu se buffer
		}
	}

	int msgv = 0;
	int msgc = 0;
	public void onIncomingMessage(ByteBuf inBuf) {
		//logger.debug("msg " + msgi++);
		final int msgt = inBuf.readInt();
		final MessageType type = MessageType.fromOrdinal(msgt);
		final int superstepNo = inBuf.readInt();
		final int fromNode = inBuf.readInt();

		if (type == MessageType.Vertex) {
			logger.debug("R Vertex message: " + msgv++);
			final int fromVertex = inBuf.readInt();
			if(!inBuf.isReadable()) {
				System.out.println("WAIT!");
				try {
					Thread.sleep(500);
				}
				catch (final InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("WAIT DONE!");
			}

			final int toVertex = inBuf.readInt();
			final int content = inBuf.readInt();
			messageListener.onIncomingVertexMessage(new VertexMessage(superstepNo, fromNode, fromVertex, toVertex, content));
		} else {
			logger.debug("R Control message: " + type + " " + msgc++);
			final int content1 = inBuf.readInt();
			final int content2 = inBuf.readInt();
			messageListener.onIncomingControlMessage(new ControlMessage(type, superstepNo, fromNode, content1, content2));
		}
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
				p.addLast(new MachineChannelHandler(activeChannels, ownId, MessageSenderAndReceiver.this));
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
				p.addLast(new MachineChannelHandler(activeChannels, ownId, MessageSenderAndReceiver.this));
			}
		});

		// Start the server.
		b.bind(port).sync();
		logger.info("Started connection server");
	}
}
