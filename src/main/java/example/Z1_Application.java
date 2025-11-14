package example;

import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

public class Z1_Application {

    public static void main(String[] args) throws Exception {

        EventLoopGroup sharedGroup = new NioEventLoopGroup(4);

        try {
            D1_SimpleWebflux webServer = new D1_SimpleWebflux(sharedGroup);
            ChannelFuture webFuture = webServer.start(8080).sync();

            E2_NettyServer randomNumberServer = new E2_NettyServer();
            randomNumberServer.start(8090).sync();

            webFuture.channel().closeFuture().sync();
        } finally {
            sharedGroup.shutdownGracefully().sync();
        }
    }
}
