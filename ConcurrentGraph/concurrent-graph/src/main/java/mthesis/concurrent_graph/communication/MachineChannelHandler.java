package mthesis.concurrent_graph.communication;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class MachineChannelHandler extends ChannelInboundHandlerAdapter {
	private static Logger logger = LoggerFactory.getLogger(MessageSenderAndReceiver.class);
	private final ConcurrentHashMap<Integer, Channel> activeChannels;
	
	
	public MachineChannelHandler(ConcurrentHashMap<Integer, Channel> activeChannels) {
		super();
		this.activeChannels = activeChannels;
	}


    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        //ctx.writeAndFlush(firstMessage);
    	logger.debug("Channel active: " + ctx.channel().id());
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        //ctx.fireChannelInactive();
    	logger.debug("Channel inactive: " + ctx.channel().id());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
    	System.out.println(msg);
        //ctx.write(msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
       ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
    	logger.error("exceptionCaught", cause);
        //cause.printStackTrace();
        ctx.close();
    }
}
