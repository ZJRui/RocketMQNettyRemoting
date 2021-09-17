package com.sachin.netty.remoting.protocol;

import com.alibaba.fastjson.annotation.JSONField;
import com.sachin.netty.remoting.CommandCustomHeader;
import com.sachin.netty.remoting.annotation.CFNotNull;
import com.sachin.netty.remoting.exception.RemotingCommandException;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author Sachin
 * @Date 2021/3/27
 **/
@Data
public class RemotingCommand {
    private static final Logger log = LoggerFactory.getLogger(RemotingCommand.class);
    public static final String SERIALIZE_TYPE_PROPERTY = "rocketmq.serialize.type";
    public static final String SERIALIZE_TYPE_ENV = "ROCKETMQ_SERIALIZE_TYPE";
    public static final String REMOTING_VERSION_KEY = "rocketmq.remoting.version";

    private static final Map<Class<? extends CommandCustomHeader>, Field[]> CLASS_HASH_MAP = new HashMap<>();
    //这是一个类的实例
    private transient CommandCustomHeader customHeader;
    private transient byte[] body;

    //存放customHeader 中属性的name和value
    private HashMap<String, String> extFields;
    private static SerializeType serializeTypeConfigInThisServer = SerializeType.ROCKETMQ;


    private String remark;

    //LanguageCode
    private LanguageCode language = LanguageCode.JAVA;
    private int code;
    private int version = 0;

    //请求的id
    private int opaque = requestId.getAndIncrement();
    /**
     * flag、RPC_TYPE，RPC_Oneway是作为一个整体使用的
     * <p>
     * 我们假定某一个属性可以有四种取值 A,B,C,D 那么我们可以使用四个位表示 ，A就是 0001,B：0010，C：0100，D:1000。为什么要用位表示而不是用1，2,3,4表示的原因
     * 在于 这个属性可以同时 既是A又是B。
     * <p>
     * 因此A可以表示成 1左移0位， B可以表示成 1左移1未， C可以表示成1左移两位
     * 因此我们定义A_TYPE=0,B_TYPE=1,C_TYPE=2,D_TYPE=3
     * A：1<<A_TYPE,  B: 1<<B_TYPE, C:1<<C_TYPE
     * <p>
     * 这个属性（假定为flag）的最开始取值是0，假定我想让这个属性包含A，那么我可以 这样做 falg=falg| 1<<A_TYPE,也就是表示falg的每一个位和（1<<A_TYPE）的每一个位进行取或运算
     * 这就导致在 flag属性包含了A值
     * <p>
     * 如果同时我想让这个flag属性也包含B， falg=(flag| (1<<B_TYPE))
     * <p>
     * 那么针对这个flag属性，我想判断他是否包含A，该如何处理呢？ 判断flag是否包含A，我们只需要判断flag的低位是否是1就可以了
     * bits=A<<A_TYPE;  （flag|bits）==bits则表示包含A
     */
    private int flag = 0;
    private static final int RPC_TYPE = 0;//0:request_command
    private static final int RPC_ONEWAY = 1;//0:RPC
    private static volatile int configVersion = -1;
    private static AtomicInteger requestId = new AtomicInteger(0);

    static {
        final String serializeProtocol = System.getProperty(SERIALIZE_TYPE_PROPERTY, System.getenv(SERIALIZE_TYPE_ENV));
        if (!isBlank(serializeProtocol)) {
            serializeTypeConfigInThisServer = SerializeType.valueOf(serializeProtocol);
            if (serializeTypeConfigInThisServer == null) {
                throw new RuntimeException("Lparse specified protocol error.protocol=" + serializeProtocol);
            }
        }

    }

    //默认的序列化方式
    private SerializeType serializeTypeCurrentRPC = serializeTypeConfigInThisServer;

    private static final Map<Field, Boolean> nullable_field_cache = new HashMap<>();
    //canonical——name_cache
    private static final Map<Class, String> CANONICAL_NAME_CACHE = new HashMap<Class, String>();
    private static final String STRING_CANONICAL_NAME = String.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_1 = Double.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_2 = double.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_1 = Integer.class.getCanonicalName();
    /**
     * 摘要：今天在看JDK中Integer.java的源码时,发现Integer.TYPE这么一个东西,publicstaticfinalClassTYPE=(Class)Class.
     * getPrimitiveClass("int");根据JDK文档的描述,Class.getPrimitiveClass("int")方法返回的是int类型的Class对象,可能很多人会疑惑,int不是基本数据类型吗?为什么还有Class对象啊?然后在网上搜寻一番之后,
     * 有9个预先定义好的Class对象代表8个基本类型和void,它们被java虚拟机创建,和基本类型有相同的名字boolean, byte, char, short, int, long, float, and double.
     * 这8个基本类型的Class对象可以通过java.lang.Boolean.TYPE,java.lang.Integer.TYPE等来访问,同样可以通过int.class,boolean.class等来访问.
     * int.class与Integer.TYPE是等价的,但是与Integer.class是不相等的,int.class指的是int的Class对象,Integer.class是Integer的Class的类对象.
     */
    private static final String INTEGER_CANONICAL_NAME_2 = int.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_1 = Long.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_2 = long.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_1 = Boolean.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_2 = boolean.class.getCanonicalName();


    /**
     * 当前RemotingCommand是请求还是响应？
     * 所谓请求是指：A 创建了RemotingCommand对象，将其设置为请求，发然后发送了出去，然后B收到这个RemotingCommand，会根据 RemotingCommand的type
     * 来判断 调用processRequestCommand还是 processResponseCommand
     * B的处理过程如下：
     * 对于processRequestCommand，那么需要找到一个Processor来处理这个RemotingCommand，
     * 对于processResponseCommand，则表示 这个数据是B之前的某一个请求 对方返回给自己的响应。此时在processResponse方法中会根据opaque找到 该请求的ResponseFuture
     * 然后将RemotingCommand中的数据作为response设置到ResponseFuture中
     * <p>
     * 默认情况下 RemotingCommand创建出来就是一个 RequestCommand。 falg属性 （默认是0，二进制0000） 中的低1位是0表示这个Command是Request
     * 低1位是1表示是Response， 低2位是1表示是oneway
     *
     * @return
     */

    @JSONField(serialize = false)
    public RemotingCommandType getType() {
        if (this.isResponseType()) {
            return RemotingCommandType.RESPONSE_COMMAND;
        }
        return RemotingCommandType.REQUEST_COMMAND;
    }

    //为什么要指定不对该该属性序列化,因为在根据接收到的数据 解码为RemotingCommand的时候 是使用了如下方法
    //com.sachin.netty.remoting.protocol.RocketMQSerializable.rocketMQProtocolDecode
    //在这个方法内直接new 了 RemotingCommand，然后网络数据包中读取数据设置到 RemotingCommand
    ///在对RemotingCommand进行 编码的时候我们也只是对 RemotingCommand中的部分属性进行编码为字节
    //包括 code version language remark 以及属性对象customHeader中的属性 所以并不是将RemotingCommand整个对象
    //进行序列化，这里为什么要加@JSONField
    @JSONField(serialize = false)
    public boolean isResponseType() {
        int bits = 1 << RPC_TYPE;
        return (this.flag & bits) == bits;
    }

    //todo 不解
    @JSONField(serialize = false)
    public boolean isOnewayRPC() {

        //将1，向左移RPC_ONEWAY位，也就是1左移1位，最终bits=2
        int bits = 1 << RPC_ONEWAY;
        //判断this.flag是否等于bits。因为只有当flag=bits时，flag&bits才会等于bits。因此只有当falg为2的时候才是oneWayRpc
        return (this.flag & bits) == bits;//注意这个地方是等于bits
    }

    public void markResponseType() {
        // System.out.println(1 << 0); 输出1，表示将1向左移0位，因此结果是没有变化
        int bits = 1 << RPC_TYPE;
        //this.flag=this.flag|bits;， this.flag 本身是0，因此this.flag与bits取或的最终结果就是bits
        //因此 如果RemotingCommand是response ，那么他的flag就是  1<<RPC_TYPE； 因此在方法isResponse中我们看到了 this.flag&(1<<RPC_TYPE)==bits
        //因此当falg为1的时候 该 Command是作为response
        //默认falg为0，此时flag表示request
        this.flag |= bits;
    }

    public void markOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        this.flag |= bits;
    }

    public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {

        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.customHeader = customHeader;
        setCmdVersion(cmd);
        return cmd;
    }

    public static RemotingCommand createResponseCommand(Class<? extends CommandCustomHeader> classHeader) {
        return createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "not set any response code", classHeader);
    }

    public static RemotingCommand createResponseCommand(int code, String remark) {

        return createResponseCommand(code, remark, null);
    }

    public static RemotingCommand createResponseCommand(int code, String remark, Class<? extends CommandCustomHeader> classHeader) {

        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.setRemark(remark);
        //todo 不解
        cmd.markResponseType();
        //todo 不解
        setCmdVersion(cmd);
        if (classHeader != null) {
            try {

                CommandCustomHeader objectHeader = classHeader.newInstance();
                cmd.customHeader = objectHeader;
            } catch (InstantiationException e) {
                e.printStackTrace();
                return null;
            } catch (IllegalAccessException e) {
                e.printStackTrace();
                return null;
            }


        }
        return cmd;
    }

    private static void setCmdVersion(RemotingCommand cmd) {

        if (configVersion >= 0) {
            cmd.setVersion(configVersion);

        } else {
            String v = System.getProperty(REMOTING_VERSION_KEY);
            if (v != null) {
                int value = Integer.parseInt(v);
                cmd.setVersion(value);
                configVersion = value;
            }
        }
    }

    public static RemotingCommand decode(final byte[] array) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(array);
        return decode(byteBuffer);
    }

    public static RemotingCommand decode(ByteBuffer byteBuffer) {

        //获取可读取上限
        int length = byteBuffer.limit();
        //获取 存储的 Header数据的长度
        int oriHeaderLen = byteBuffer.getInt();
        int headerLength = getHeaderLength(oriHeaderLen);

        byte[] headerData = new byte[headerLength];
        byteBuffer.get(headerData);
        RemotingCommand remotingCommand = headerDecode(headerData, getProtocolType(oriHeaderLen));

        //-4是因为HeaderData的长度字段占用四个字节
        int bodyLength = length - 4 - headerLength;
        byte[] bodyData = null;
        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            byteBuffer.get(bodyData);
        }
        remotingCommand.setBody(bodyData);
        return remotingCommand;


    }

    private static RemotingCommand headerDecode(byte[] headerData, SerializeType type) {
        switch (type) {
            case JSON:

                return null;
            case ROCKETMQ:
                RemotingCommand remotingCommand = RocketMQSerializable.rocketMQProtocolDecode(headerData);
                remotingCommand.setSerializeTypeCurrentRPC(type);
                return remotingCommand;
            default:
                break;
        }

        return null;

    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public byte[] getBody() {
        return body;
    }

    private void setSerializeTypeCurrentRPC(SerializeType type) {
        this.serializeTypeCurrentRPC = type;
    }

    private static SerializeType getProtocolType(int source) {
        //source向右移动24位，也就是只取source的高8位，舍弃低24位
        return SerializeType.valueOf((byte) ((source >> 24) & 0xFF));
    }

    private static int getHeaderLength(int oriHeaderLen) {
        //Header的长度 是int 占用了4个字节，但是第一个字节用来存储了序列化的类型，第2-4字节存储HeaderData的长度信息
        //且长度数字采用了大端存储的方式。因此这里我们需要取出 oriHeaderLen的2-4字节
        //在这里一个F表示四位， 6个F表示 取出oriHeaderLen的低三位
        //todo 不应该是取高三位的吗？
        return oriHeaderLen & 0xFFFFFF;
    }

    public ByteBuffer encodeHeader() {

        return encodeHeader(this.body != null ? this.body.length : 0);
    }

    public ByteBuffer encodeHeader(final int bodyLength) {
        //头部信息和body信息

        //1,header length size
        int length = 4;
        //2>header data length
        byte[] headerData;
        //header信息包含：（1）当前RemotingCommand对象的 code、verions、language属性（2）当前对象的customHeader属性对象的属性转成的map 集合extFields
        headerData = this.headerEncode();
        length += headerData.length;
        //3> body data length。 当前remotingCommand编码之后的结果是 Header+body
        length += bodyLength;

        //头部Header的数据包含： RemotingCommand对象编码后 所占用的总长度大小、 头部数据的大小、头部数据本身内容
        //因此下面的中的第一个4 就表示 RemotingCommand编码后整体的大小。 length-bodylength就是 头部数据的大小字段+头部数据本身内容占用的字节
        ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

        //设置RemotingCommand编码后整个数据包的长度
        result.putInt(length);
        //设置header头部的长度(头部header的长度包含两部分数据 ：头部数据的长度 和序列化信息)
        result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        result.put(headerData);

        //将写入模式转换成读模式，（1） 设置可读上限limit为当前缓冲区中内容最后写入位置position，（2） 把读的起始位置position设置为0，表示从头开始读取
        //(3)清除之前的mark标记
        result.flip();

        return result;
    }

    public static byte[] markProtocolType(int source, SerializeType type) {

        byte[] result = new byte[4];
        //第一个字节存储 序列化方式信息
        result[0] = type.getCode();
        //首先我们要都知道, &表示按位与,只有两个位同时为1,才能得到1, 0x代表16进制数,0xff表示的数二进制1111 1111 占一个字节.和其进行&操作的数,最低8位,不会发生变化.
        //TCPIP协议中使用的字节序通常称为网络字节序，TCPIP协议将字节序定义为大端
        //取出整型 source的高位 放在result的低位, source向右移16为，则表示去除低16位，剩下高16位，然后取出高16为中的低8位
        result[1] = (byte) ((source >> 16) & 0xFF);
        //取出source的低第二个字节
        result[2] = (byte) ((source >> 8) & 0xFF);
        //取出source的低第一个字节
        result[3] = (byte) ((source & 0xFF));
        return result;
    }


    private byte[] headerEncode() {
        //将属性customHeader的属性存放到extFields中
        //这里有个问题：字段body属性也被放入到extFields中，因此body也是作为header吗？不是的，body是当前对象的属性，headerEncode地对当前
        //对象的属性customheader对象的属性编码成byte数组
        //注意这里是将属性 customHeader对象的 字段方锐到extFields中。 并不是将当前对象的属性字段放入到extFields中

        this.makeCustomHaderToNet();
        //将当前对象的属性信息编码为Byte数组
        if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC) {
            //RocketMQSerializable 编码的时候 针对当前对象的 code、version、language、以及extFields信息进行编码成Byte数组
            //当前对象的body属性并没有被编码
            return RocketMQSerializable.rocketMQProtocolEncode(this);
        } else {

        }

        return null;

    }

    private void makeCustomHaderToNet() {
        if (this.customHeader != null) {
            //或者这个类的所有字段
            Field[] fields = getClazzFields(customHeader.getClass());
            if (this.extFields == null) {
                this.extFields = new HashMap<>();
            }
            for (Field field : fields) {
                //非静态属性
                if (!Modifier.isStatic(field.getModifiers())) {
                    String name = field.getName();
                    if (!name.startsWith("this")) {
                        Object value = null;
                        try {
                            field.setAccessible(true);
                            //获取自定义头对象上的属性值
                            value = field.get(customHeader);
                        } catch (IllegalAccessException e) {
                            log.error("Failed to access field [{}]", name, e);
                        }
                        if (value != null) {
                            this.extFields.put(name, value.toString());
                        }
                    }
                }

            }
        }

    }

    private Field[] getClazzFields(Class<? extends CommandCustomHeader> classHeader) {
        Field[] fields = CLASS_HASH_MAP.get(classHeader);
        if (fields == null) {
            /**
             * getDeclaredFields()：返回反映由类对象表示的类或接口声明的所有字段的字段对象数组。这包括公共、受保护、默认（包）访问和专用字段，但不包括继承的字段。
             * 而getDeclaredFields()只能获取自己声明的各种字段，包括public，protected，private。接口有两种使用方式（1）创建一个类实现接口，此时
             * 该类的getDeclaredFields只返回类自己声明的field，不会返回接口中的field。 （2）new 一个接口的实现，此时该对象返回接口的field
             *
             * getFields()：返回一个数组，其中包含反映该类对象表示的类或接口的所有可访问公共字段的字段，包括继承的字段，但是只能是public。
             */
            fields = classHeader.getDeclaredFields();
            synchronized (CLASS_HASH_MAP) {
                CLASS_HASH_MAP.put(classHeader, fields);
            }
        }
        return fields;
    }

    private static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public CommandCustomHeader decodeCommandCustomHeader(Class<? extends CommandCustomHeader> classHeader) throws RemotingCommandException {

        CommandCustomHeader objectHeader;
        try {
            objectHeader = classHeader.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
            return null;
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            return null;
        }
        if (this.extFields != null) {
            Field[] fields = getClazzFields(classHeader);
            for (Field field : fields) {
                String fieldName = field.getName();
                try {
                    if (Modifier.isStatic(field.getModifiers())) {
                        continue;
                    }
                    if (fieldName.startsWith("this")) {
                        continue;
                    }
                    String value = this.extFields.get(fieldName);
                    if (value == null) {
                        //判断field是否可以为空
                        if (!isFieldNullable(field)) {
                            //field不可以为空，但是value却是空
                            throw new RemotingCommandException("the custom field<" + fieldName + "> is Null");
                        }
                        continue;
                    }
                    field.setAccessible(true);

                    String type = getCanonicalName(field.getType());
                    Object valueParsed = null;
                    if (type.equals(STRING_CANONICAL_NAME)) {
                        valueParsed = value;
                    }
                    if (type.equals(INTEGER_CANONICAL_NAME_1) || type.equals(INTEGER_CANONICAL_NAME_2)) {
                        valueParsed = Integer.parseInt(value);
                    }
                    if (type.equals(LONG_CANONICAL_NAME_1) || type.equals(LONG_CANONICAL_NAME_2)) {
                        valueParsed = Long.parseLong(value);
                    }
                    if (type.equals(BOOLEAN_CANONICAL_NAME_1) || type.equals(BOOLEAN_CANONICAL_NAME_2)) {
                        valueParsed = Boolean.valueOf(value);
                    }
                    if (type.equals(DOUBLE_CANONICAL_NAME_1) || type.equals(DOUBLE_CANONICAL_NAME_2)) {
                        valueParsed = Double.parseDouble(value);
                    }
                    if (valueParsed == null) {
                        throw new RemotingCommandException("the custom field<" + fieldName + " >type is not support");
                    }
                    field.set(objectHeader, valueParsed);
                } catch (Throwable e) {
                    log.error("failed field[{}] decoding", fieldName, e);
                }

            }
            objectHeader.checkFields();

        }

        return objectHeader;
    }

    private boolean isFieldNullable(Field field) {
        if (!nullable_field_cache.containsKey(field)) {
            //如果存在注解表示不可以为空，因此会在map中存储field false.
            Annotation annotation = field.getAnnotation(CFNotNull.class);
            synchronized (nullable_field_cache) {
                nullable_field_cache.put(field, annotation == null);
            }
        }
        return nullable_field_cache.get(field);
    }

    private String getCanonicalName(Class clazz) {
        String name = CANONICAL_NAME_CACHE.get(clazz);

        if (name == null) {
            name = clazz.getCanonicalName();
            synchronized (CANONICAL_NAME_CACHE) {
                CANONICAL_NAME_CACHE.put(clazz, name);
            }
        }
        return name;
    }

    @Override
    public String toString() {
        return "RemotingCommand [code=" + code + ", language=" + language + ", version=" + version + ", opaque=" + opaque + ", flag(B)="
                + Integer.toBinaryString(flag) + ", remark=" + remark + ", extFields=" + extFields + ", serializeTypeCurrentRPC="
                + serializeTypeCurrentRPC + "]";
    }


}
