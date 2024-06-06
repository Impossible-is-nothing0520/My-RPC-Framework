package top.guoziyang.rpc.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.guoziyang.rpc.entity.RpcRequest;
import top.guoziyang.rpc.entity.RpcResponse;
import top.guoziyang.rpc.io.Bytes;
import top.guoziyang.rpc.serializer.CommonSerializer;

import java.io.IOException;
import java.util.List;

/**
 * 通用的解码拦截器
 *
 * @author ziyang
 */
public class DubboDecoder extends ReplayingDecoder {

    private static final Logger logger = LoggerFactory.getLogger(DubboDecoder.class);
    private static final int MAGIC_NUMBER = 0xCAFEBABE;

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


    private final CommonSerializer serializer;

    public DubboDecoder(CommonSerializer serializer) {
        this.serializer = serializer;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
        // 解码
        int readable = buffer.readableBytes();
        // 创建消息头字节数组
        byte[] header = new byte[Math.min(readable, HEADER_LENGTH)];
        // 读取消息头数据
        buffer.readBytes(header);
        // 调用重载方法进行后续解码工作
        decode(ctx, buffer, readable, header, out);
    }

    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, int readable, byte[] header, List<Object> out) throws IOException {
        /**
         * 通过检测消息头中的魔数是否与规定的魔数相等，提前拦截掉非常规数据包，比如通过 telnet 命令行发出的数据包。
         * 接着再对消息体长度，以及可读字节数进行检测。最后调用 decodeBody 方法进行后续的解码工作，
         * ExchangeCodec 中实现了 decodeBody 方法，但因其子类 DubboCodec 覆写了该方法，
         * 所以在运行时 DubboCodec 中的 decodeBody 方法会被调用。
         */
        // 检查魔数是否相等
        // check magic number.
        if (readable > 0 && header[0] != MAGIC_HIGH || readable > 1 && header[1] != MAGIC_LOW) {
            int length = header.length;
            if (header.length < readable) {
                header = Bytes.copyOf(header, readable);
                buffer.readBytes(header, length, readable - length);
            }
            for (int i = 1; i < header.length - 1; i++) {
                if (header[i] == MAGIC_HIGH && header[i + 1] == MAGIC_LOW) {
                    buffer.readerIndex(buffer.readerIndex() - header.length + i);
                    header = Bytes.copyOf(header, i);
                    break;
                }
            }
        }

        decodeBody(buffer, header, out);

    }

    protected void decodeBody(ByteBuf buffer, byte[] header, List<Object> out) throws IOException {
        byte flag = header[2], proto = (byte) (flag & SERIALIZATION_MASK);
        // get request id.
        long id = Bytes.bytes2long(header, 4);
        if ((flag & FLAG_REQUEST) == 0) {
            // decode response.
            // decode request.
            int bodyLength = Bytes.bytes2int(header, 12);
            // Read the body
            byte[] body = new byte[bodyLength];
            buffer.readBytes(body);
            Object res = serializer.deserialize(body, RpcResponse.class);
            out.add(res);

        } else {
            // decode request.
            int bodyLength = Bytes.bytes2int(header, 12);
            // Read the body
            byte[] body = new byte[bodyLength];
            buffer.readBytes(body);

            // Deserialize the request data
            Object req = serializer.deserialize(body, RpcRequest.class);
            out.add(req);
        }
    }


}
