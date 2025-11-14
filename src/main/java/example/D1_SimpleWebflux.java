package example;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import reactor.core.publisher.Mono;

public class D1_SimpleWebflux {
    private final EventLoopGroup sharedGroup;
    private final Z2_DependencyInjectionManager diManager;

    public D1_SimpleWebflux(EventLoopGroup sharedGroup) {
        this.sharedGroup = sharedGroup;

        Z2_DependencyInjectionManager diManager = new Z2_DependencyInjectionManager();
        diManager.setWebClient(new F1_SimpleWebClient(sharedGroup));
        diManager.setController(new Z3_Controller(diManager.getWebClient()));

        this.diManager = diManager;
    }

    public ChannelFuture start(int port) {

        ServerBootstrap serverBootstrap = new ServerBootstrap();

        serverBootstrap.group(sharedGroup, sharedGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new StringDecoder(CharsetUtil.UTF_8), new StringEncoder(CharsetUtil.UTF_8));
                        ch.pipeline().addLast(new SimpleWebfluxHandler(diManager.getController()));
                    }
                });
        return serverBootstrap.bind(port);
    }
}

class SimpleWebfluxHandler extends SimpleChannelInboundHandler<String> {
    private final Z3_Controller controller;

    public SimpleWebfluxHandler(Z3_Controller controller) {
        this.controller = controller;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) {
        Mono<String> asyncMono = controller.process(msg);

        asyncMono.subscribe(
                result -> {
                    ctx.writeAndFlush("응답: " + result + "\n");
                },
                error -> {
                    ctx.writeAndFlush("에러: " + error.getMessage() + "\n");
                }
        );
    }
}