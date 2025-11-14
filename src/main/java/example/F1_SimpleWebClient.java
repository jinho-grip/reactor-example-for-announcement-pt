package example;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;


public class F1_SimpleWebClient {

    private final EventLoopGroup group;
    private final Bootstrap baseBootstrap;

    public F1_SimpleWebClient(EventLoopGroup sharedGroup) {
        this.group = sharedGroup;
        this.baseBootstrap = new Bootstrap()
                .group(this.group)
                .channel(NioSocketChannel.class);
    }

    public Mono<String> sendMessage(String host, int port, String message) {
        return Mono.create(sink -> {
            Bootstrap callBootstrap = baseBootstrap.clone();
            callBootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(
                            new StringDecoder(CharsetUtil.UTF_8),
                            new StringEncoder(CharsetUtil.UTF_8),
                            new SimpleClientHandler(message, sink)
                    );
                }
            });

            callBootstrap.connect(host, port);
        });
    }
}

class SimpleClientHandler extends SimpleChannelInboundHandler<String> {

    private final String messageToSend;
    private final MonoSink<String> sink; // Mono에 결과를 전달할 통로

    public SimpleClientHandler(String messageToSend, MonoSink<String> sink) {
        this.messageToSend = messageToSend;
        this.sink = sink;
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.writeAndFlush(messageToSend);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        sink.success(msg);
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        sink.error(cause);
        ctx.close();
    }
}
