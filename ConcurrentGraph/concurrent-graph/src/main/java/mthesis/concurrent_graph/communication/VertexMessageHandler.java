package mthesis.concurrent_graph.communication;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import mthesis.concurrent_graph.communication.Messages.VertexMessage;


public class VertexMessageHandler extends SimpleChannelInboundHandler<VertexMessage> {
	//private final Logger logger;
	private final MessageSenderAndReceiver messageListner;

	public VertexMessageHandler(int ownId, MessageSenderAndReceiver messageListner) {
		super();
		//this.logger = LoggerFactory.getLogger(VertexMessageHandler.class.getCanonicalName() + "[" + ownId + "]");
		this.messageListner = messageListner;
	}



	@Override
	protected void channelRead0(ChannelHandlerContext ctx, VertexMessage msg) throws Exception {
		messageListner.onIncomingVertexMessage(msg);
	}
}
