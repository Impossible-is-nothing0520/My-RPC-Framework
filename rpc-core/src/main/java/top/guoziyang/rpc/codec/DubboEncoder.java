package top.guoziyang.rpc.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.guoziyang.rpc.entity.RpcRequest;
import top.guoziyang.rpc.entity.RpcResponse;
import top.guoziyang.rpc.io.Bytes;
import top.guoziyang.rpc.serializer.CommonSerializer;

import java.io.IOException;


public class DubboEncoder extends MessageToByteEncoder {

    private static final Logger log = LoggerFactory.getLogger(DubboEncoder.class);

    // header length.
    protected static final int HEADER_LENGTH = 16;
    // magic header.
    protected static final short MAGIC = (short) 0xdabb;
    protected static final byte MAGIC_HIGH = Bytes.short2bytes(MAGIC)[0];
    protected static final byte MAGIC_LOW = Bytes.short2bytes(MAGIC)[1];
    // message flag.
    protected static final byte FLAG_REQUEST = (byte) 0x80;
    protected static final byte FLAG_TWOWAY = (byte) 0x40;
    protected static final byte FLAG_EVENT = (byte) 0x20;
    protected static final int SERIALIZATION_MASK = 0x1f;

    public static final String NAME = "dubbo";
    public static final String DUBBO_VERSION = "2.0.2";
    public static final byte RESPONSE_WITH_EXCEPTION = 0;
    public static final byte RESPONSE_VALUE = 1;
    public static final byte RESPONSE_NULL_VALUE = 2;
    public static final byte RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS = 3;
    public static final byte RESPONSE_VALUE_WITH_ATTACHMENTS = 4;
    public static final byte RESPONSE_NULL_VALUE_WITH_ATTACHMENTS = 5;
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
    public static final Class<?>[] EMPTY_CLASS_ARRAY = new Class<?>[0];

    private static final int MAGIC_NUMBER = 0xCAFEBABE;

    private final CommonSerializer serializer;

    public DubboEncoder(CommonSerializer serializer) {
        this.serializer = serializer;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        //编码
        if (msg instanceof RpcRequest) {
            // 对 Request 对象进行编码  consumer发送消息时用到
            encodeRequest(ctx, out, (RpcRequest) msg);
        } else if (msg instanceof RpcResponse) {
            // 对 Response 对象进行编码  provider响应消息时用到
            encodeResponse(ctx, out, (RpcResponse) msg);
        }
    }

    private void encodeResponse(ChannelHandlerContext ctx, ByteBuf buffer, RpcResponse res) {
        int savedWriteIndex = buffer.writerIndex();
        try {
            // header.
            // 创建消息头字节数组
            byte[] header = new byte[HEADER_LENGTH];
            // set magic number.
            // 设置魔数
            Bytes.short2bytes(MAGIC, header);
            // set request and serialization flag.
            // 设置序列化器编号
            header[2] = 2;

            // set response status.
            // 获取响应状态
            byte status = res.getStatus();
            // 设置响应状态
            header[3] = status;
            // set request id.
            // 设置请求编号
            Bytes.long2bytes(res.getMId(), header, 4);
            // 更新 writerIndex，为消息头预留 16 个字节的空间
            buffer.writerIndex(savedWriteIndex + HEADER_LENGTH);


            byte[] bytes = serializer.serialize(res);
            int len = bytes.length;

            buffer.writeBytes(bytes);

            // 将消息体长度写入到消息头中
            Bytes.int2bytes(len, header, 12);
            // write
            // 将 buffer 指针移动到 savedWriteIndex，为写消息头做准备
            buffer.writerIndex(savedWriteIndex);
            // 从 savedWriteIndex 下标处写入消息头
            buffer.writeBytes(header); // write header.
            // 设置新的 writerIndex，writerIndex = 原写下标 + 消息头长度 + 消息体长度
            buffer.writerIndex(savedWriteIndex + HEADER_LENGTH + len);
        } catch (Exception e) {

        }
    }

    private void encodeRequest(ChannelHandlerContext ctx, ByteBuf buffer, RpcRequest req) throws IOException {
        /**
         * 首先会通过位运算将消息头写入到 header 数组中。然后对 Request 对象的 data 字段执行序列化操作，
         * 序列化后的数据最终会存储到 ChannelBuffer 中。序列化操作执行完后，可得到数据序列化后的长度 len，紧接着将 len 写入到
         * header 指定位置处。最后再将消息头字节数组 header 写入到 ChannelBuffer 中，整个编码过程就结束了
         */

        /**
         * +---------------------------------------------------------------+
         * | 16bit magic | 8bit flag | 8bit status | 64bit request id     |
         * +---------------------------------------------------------------+
         * | 32bit body length                                           |
         * +---------------------------------------------------------------+
         * | body (object)                                              |
         * +---------------------------------------------------------------+
         */
        // header.   创建消息头字节数组，长度为 16
        byte[] header = new byte[HEADER_LENGTH];
        // set magic number.
        // 设置魔数
        Bytes.short2bytes(MAGIC, header); // todo 2个字节 0，1

        // set request and serialization flag.
        // 设置数据包类型（Request/Response）和序列化器编号
        header[2] = (byte) (FLAG_REQUEST | 2);

        // 设置通信方式(单向/双向)
        header[2] |= FLAG_TWOWAY;

        // set request id.
        // 设置请求编号，8个字节，从第4个字节开始设置
        Bytes.long2bytes(req.getMId(), header, 4); // todo 第4个字节开始设置  8个字节

        // encode request data.
        // 获取 buffer 当前的写位置
        int savedWriteIndex = buffer.writerIndex();
        // 更新 writerIndex，为消息头预留 16 个字节的空间
        buffer.writerIndex(savedWriteIndex + HEADER_LENGTH);

        // 对请求数据进行序列化操作
        //encodeRequestData(ctx, buffer, req);

        byte[] bytes = serializer.serialize(req);
        int len = bytes.length;

        buffer.writeBytes(bytes);
        // 将消息体长度写入到消息头中
        Bytes.int2bytes(bytes.length, header, 12);

        // write
        // 将 buffer 指针移动到 savedWriteIndex，为写消息头做准备
        buffer.writerIndex(savedWriteIndex);
        // 从 savedWriteIndex 下标处写入消息头
        buffer.writeBytes(header); // write header.
        // 设置新的 writerIndex，writerIndex = 原写下标 + 消息头长度 + 消息体长度
        buffer.writerIndex(savedWriteIndex + HEADER_LENGTH + len);
    }


}
