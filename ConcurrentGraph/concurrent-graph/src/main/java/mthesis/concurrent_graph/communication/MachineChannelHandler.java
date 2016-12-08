package mthesis.concurrent_graph.communication;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;


// TODO Everything should be more robust, error handling etc.
public class MachineChannelHandler extends ChannelInboundHandlerAdapter {
	private final Logger logger;
	private final int ownId;
	private final MessageSenderAndReceiver messageListner;
	private final ConcurrentHashMap<Integer, Channel> activeChannels;

	private enum ChannelState { Inactive, Handshake, Active }
	private ChannelState channelState = ChannelState.Inactive;
	private int connectedMachine;

	//private ByteBuf buffer;


	public MachineChannelHandler(ConcurrentHashMap<Integer, Channel> activeChannels, int ownId,
			MessageSenderAndReceiver messageListner) {
		super();
		this.ownId = ownId;
		this.logger = LoggerFactory.getLogger(MachineChannelHandler.class.getCanonicalName() + "[" + ownId + "]");
		this.activeChannels = activeChannels;
		this.messageListner = messageListner;
	}


	@Override
	public void channelActive(ChannelHandlerContext ctx) {
		// ctx.writeAndFlush(firstMessage);
		logger.debug("Channel active: " + ctx.channel().id());
		if (channelState == ChannelState.Inactive) {
			channelState = ChannelState.Handshake;
			final ByteBuf outBuf = ctx.alloc().buffer(4); // (2)
			outBuf.writeInt(ownId);
			ctx.writeAndFlush(outBuf);
		} else {
			logger.warn("Channel not inactive ignoring active channel " + ctx.channel().id());
		}
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		//ctx.fireChannelInactive();
		channelState = ChannelState.Inactive;
		logger.debug("Channel inactive: " + ctx.channel().id());
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
		//logger.trace("channelRead " + ctx.channel().id() + " " + msg);
		try {
			final ByteBuf inBuf = (ByteBuf) msg;
			if (channelState == ChannelState.Active) {
				messageListner.onIncomingMessage(inBuf);
			} else if (channelState == ChannelState.Handshake) {
				//				int i =1;
				while (inBuf.isReadable()){
					//					System.out.println("a " + i++);
					connectedMachine = inBuf.readInt();
					activeChannels.put(connectedMachine, ctx.channel());
					channelState = ChannelState.Active;
					logger.debug("Channel handshake finished. Connected " + connectedMachine + " via " + ctx.channel().id());
				}
				//				System.out.println("b " + i);
			}
		} finally {
			ReferenceCountUtil.release(msg);
		}
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
		ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		// Close the connection when an exception is raised.
		logger.error("exceptionCaught", cause);
		ctx.close();
	}
}
