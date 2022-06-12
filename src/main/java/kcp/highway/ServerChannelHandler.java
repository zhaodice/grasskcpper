package kcp.highway;

import io.netty.buffer.Unpooled;
import kcp.highway.erasure.fec.Fec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kcp.highway.threadPool.IMessageExecutor;
import kcp.highway.threadPool.IMessageExecutorPool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by JinMiao
 * 2018/9/20.
 */
public class ServerChannelHandler extends ChannelInboundHandlerAdapter {
    record HandshakeWaiter(long convId, InetSocketAddress address){

    }
    private static final Logger logger = LoggerFactory.getLogger(ServerChannelHandler.class);

    private final IChannelManager channelManager;

    private final ChannelConfig channelConfig;

    private final IMessageExecutorPool iMessageExecutorPool;

    private final KcpListener kcpListener;

    private final HashedWheelTimer hashedWheelTimer;
    private final ConcurrentLinkedQueue<HandshakeWaiter> handshakeWaiters = new ConcurrentLinkedQueue<>();
    public void handshakeWaitersAppend(HandshakeWaiter handshakeWaiter){
        if(handshakeWaiters.size()>10){
            handshakeWaiters.poll();
        }
        handshakeWaiters.add(handshakeWaiter);
    }
    public void handshakeWaitersRemove(HandshakeWaiter handshakeWaiter){
        handshakeWaiters.remove(handshakeWaiter);
    }
    public HandshakeWaiter handshakeWaitersFind(long conv){
        for (HandshakeWaiter waiter : handshakeWaiters) {
            if (waiter.convId == conv) {
                return waiter;
            }
        }
        return null;
    }
    public HandshakeWaiter handshakeWaitersFind(InetSocketAddress address){
        for (HandshakeWaiter waiter : handshakeWaiters) {
            if (waiter.address.equals(address)) {
                return waiter;
            }
        }
        return null;
    }
    // Handle handshake
    public static void handleEnet(ByteBuf data, User user,Ukcp ukcp, long conv) {
        if (data == null || data.readableBytes() != 20) {
            return;
        }
        // Get
        int code = data.readInt();
        data.readUnsignedInt(); // Empty
        data.readUnsignedInt(); // Empty
        int enet = data.readInt();
        data.readUnsignedInt();
        try{
            switch (code) {
                case 255 -> { // Connect + Handshake
                    sendHandshakeRsp(user,enet,conv);
                }
                case 404 -> { // Disconnect
                    sendDisconnectPacket(user, 1,conv);
                    if(ukcp!=null) {
                        ukcp.close();
                    }
                }
            }
        }catch (Throwable ignore){
        }
    }

    private static void sendHandshakeRsp(User user,int enet,long conv) throws IOException {
        ByteBuf packet = Unpooled.buffer(20);
        packet.writeInt(325);
        packet.writeIntLE((int) (conv >> 32));
        packet.writeIntLE((int) (conv & 0xFFFFFFFFL));
        packet.writeInt(enet);
        packet.writeInt(340870469); // constant?
        UDPSend(user,packet);
    }
    public static void sendDisconnectPacket(User user, int code,long conv) throws IOException {
        ByteBuf packet = Unpooled.buffer(20);
        packet.writeInt(404);
        packet.writeIntLE((int) (conv >> 32));
        packet.writeIntLE((int) (conv & 0xFFFFFFFFL));
        packet.writeInt(code);
        packet.writeInt(423728276); // constant?
        UDPSend(user,packet);
    }
    private static void UDPSend(User user,ByteBuf packet){
        DatagramPacket datagramPacket = new DatagramPacket(packet,user.getRemoteAddress(), user.getLocalAddress());
        // Send
        user.getChannel().writeAndFlush(datagramPacket);
    }

    public ServerChannelHandler(IChannelManager channelManager, ChannelConfig channelConfig, IMessageExecutorPool iMessageExecutorPool, KcpListener kcpListener,HashedWheelTimer hashedWheelTimer) {
        this.channelManager = channelManager;
        this.channelConfig = channelConfig;
        this.iMessageExecutorPool = iMessageExecutorPool;
        this.kcpListener = kcpListener;
        this.hashedWheelTimer = hashedWheelTimer;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("", cause);
        //SocketAddress socketAddress = ctx.channel().remoteAddress();
        //Ukcp ukcp = clientMap.get(socketAddress);
        //if(ukcp==null){
        //    logger.error("exceptionCaught ukcp is not exist address"+ctx.channel().remoteAddress(),cause);
        //    return;
        //}
        //ukcp.getKcpListener().handleException(cause,ukcp);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object object) {
        final ChannelConfig channelConfig = this.channelConfig;
        DatagramPacket msg = (DatagramPacket) object;
        ByteBuf byteBuf = msg.content();
        User user = new User(ctx.channel(), msg.sender(), msg.recipient());
        Ukcp ukcp = channelManager.get(msg);
        if(byteBuf.readableBytes() == 20){
            // send handshake
            HandshakeWaiter waiter = handshakeWaitersFind(user.getRemoteAddress());
            long convId;
            if(waiter==null) {
                convId = new Random().nextLong();
                handshakeWaitersAppend(new HandshakeWaiter(convId, user.getRemoteAddress()));
            }else{
                convId = waiter.convId;
            }
            handleEnet(byteBuf, user, ukcp, convId);
            msg.release();
            return;
        }
        IMessageExecutor iMessageExecutor = iMessageExecutorPool.getIMessageExecutor();
        if (ukcp == null) {// finished handshake
            HandshakeWaiter waiter = handshakeWaitersFind(byteBuf.getLong(0));
            if (waiter == null) {
                //Grasscutter.getLogger().warn("Establishing handshake to {} failure, Conv id Error", user.getRemoteAddress());
                msg.release();
                return;
            } else {
                handshakeWaitersRemove(waiter);
                int sn = getSn(byteBuf, channelConfig);
                if (sn != 0) {
                    //Grasscutter.getLogger().warn("Establishing handshake to {} failure, SN!=0", user.getRemoteAddress());
                    msg.release();
                    return;
                }
                //Grasscutter.getLogger().info("Established handshake to {} ,Conv convId={}", user.getRemoteAddress(), waiter.convId);
                KcpOutput kcpOutput = new KcpOutPutImp();
                Ukcp newUkcp = new Ukcp(kcpOutput, kcpListener, iMessageExecutor, channelConfig, channelManager);
                newUkcp.user(user);
                newUkcp.setConv(waiter.convId);
                channelManager.New(msg.sender(), newUkcp, msg);
                iMessageExecutor.execute(() -> {
                    try {
                        newUkcp.getKcpListener().onConnected(newUkcp);
                    } catch (Throwable throwable) {
                        newUkcp.getKcpListener().handleException(throwable, newUkcp);
                    }
                });
                hashedWheelTimer.newTimeout(new ScheduleTask(iMessageExecutor, newUkcp, hashedWheelTimer),
                        newUkcp.getInterval(),
                        TimeUnit.MILLISECONDS);
                ukcp = newUkcp;
            }
        }
        // established tunnel
        Ukcp finalUkcp = ukcp;
        iMessageExecutor.execute(() -> {
            finalUkcp.user().setRemoteAddress(msg.sender());
            finalUkcp.read(byteBuf);
        });
    }


    private int getSn(ByteBuf byteBuf,ChannelConfig channelConfig){
        int headerSize = 0;
        if(channelConfig.getFecAdapt()!=null){
            headerSize+= Fec.fecHeaderSizePlus2;
        }
        return byteBuf.getIntLE(byteBuf.readerIndex()+Kcp.IKCP_SN_OFFSET+headerSize);
    }

}
