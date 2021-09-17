package com.sachin.netty.remoting.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @Author Sachin
 * @Date 2021/3/27
 **/
public class RocketMQSerializable {

    private static final Charset CHARSET_URT8 = Charset.forName("UTF-8");


    /**
     * 将对象转为 字节数组
     * 生成头部信息
     *
     * @param cmd
     * @return
     */
    public static byte[] rocketMQProtocolEncode(RemotingCommand cmd) {

        //String remark
        byte[] remarkBytes = null;
        int remarkLen = 0;
        if (cmd.getRemark() != null && cmd.getRemark().length() > 0) {
            remarkBytes = cmd.getRemark().getBytes(CHARSET_URT8);
            remarkLen = -remarkBytes.length;
        }
        //HashMap<String,String> extFields; 这个map是RemotingCommand的属性customHeader对象的属性转成了map
        byte[] extFieldsBytes = null;
        int extLen = 0;
        if (cmd.getExtFields() != null && !cmd.getExtFields().isEmpty()) {
            //将map序列化为byte
            extFieldsBytes = mapSerialize(cmd.getExtFields());
            extLen = extFieldsBytes.length;

        }

        int totalLen = calTotalLen(remarkLen, extLen);

        ByteBuffer headerBuffer = ByteBuffer.allocate(totalLen);
        //int code (~32767)，code是int类型，但是这里我们只给他分配两个字节
        headerBuffer.putShort((short) cmd.getCode());
        //LanguageCode language
        headerBuffer.put(cmd.getLanguage().getCode());
        //int version 两个字节
        headerBuffer.putShort((short) cmd.getVersion());
        //int opaque
        headerBuffer.putInt(cmd.getOpaque());
        // int flag
        headerBuffer.putInt(cmd.getFlag());
        //String remark
        if (remarkBytes != null) {
            //放入长度和数据
            headerBuffer.putInt(remarkBytes.length);
            headerBuffer.put(remarkBytes);
        } else {
            //记录remark数据的长度为0
            headerBuffer.putInt(0);
        }
        //HashMap<String,String> extFields;
        if (extFieldsBytes != null) {
            headerBuffer.putInt(extFieldsBytes.length);
            headerBuffer.put(extFieldsBytes);
        } else {
            headerBuffer.putInt(0);
        }
        return headerBuffer.array();

    }

    private static int calTotalLen(int remarkLen, int extLen) {
        //int code(~32767)

        int length = 2
                //LanguageCode 占用一个字节
                + 1
                // int version占用两个字节,虽然 cmd中的版本信息使用了int字段，但是实际他数字是有范围的，所以只会占用两个字节，在上面分配内存的时候我们看到只分配了两个字节
                + 2
                //int opaque
                + 4
                //int flag
                + 4
                //String remark,下面中的第一个4表示remark数据的长度，remarkLen表示具体的数据会占用多少个字节
                + 4 + remarkLen
                //HashMap<String,String> extFields;
                + 4 + extLen;
        return length;
    }

    //将map数据序列化为字节数组
    public static byte[] mapSerialize(HashMap<String, String> map) {
        //keySize+key+valueSize+value
        if (null == map || map.isEmpty()) {
            return null;
        }
        int totalLength = 0;
        int kvLength;
        Iterator<Map.Entry<String, String>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            if (entry.getKey() != null && entry.getValue() != null) {
                kvLength =
                        //keSize+key
                        2 + entry.getKey().getBytes(CHARSET_URT8).length
                                //valueSize+val
                                + 4 + entry.getValue().getBytes(CHARSET_URT8).length;

                totalLength += kvLength;
            }

        }
        ByteBuffer content = ByteBuffer.allocate(totalLength);
        byte[] key;
        byte[] val;
        Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            if (entry.getKey() != null && entry.getValue() != null) {
                key = entry.getKey().getBytes(CHARSET_URT8);
                val = entry.getValue().getBytes(CHARSET_URT8);
                //为什么这个地方是putShort? 这个地方放入的数据其实是keySize，short占用就是2字节
                content.putShort((short) key.length);
                content.put(key);
                //int占用的是4字节，因此前面我们计算 kvLength的时候就是使用了value占用4字节
                content.putInt(val.length);
                content.put(val);
            }
        }

        return content.array();
    }


    /**
     * 字节数据反序列化为对象数据
     *
     * @param headerArray
     * @return
     */
    public static RemotingCommand rocketMQProtocolDecode(final byte[] headerArray) {
        RemotingCommand cmd = new RemotingCommand();
        ByteBuffer headerBuffer = ByteBuffer.wrap(headerArray);
        //int code(~32767) 两个字节的code
        cmd.setCode(headerBuffer.getShort());
        //Language code 一个字节
        cmd.setLanguage(LanguageCode.valueOf(headerBuffer.get()));
        //int version 占用两个字节
        cmd.setVersion(headerBuffer.getShort());
        //int opaque
        cmd.setOpaque(headerBuffer.getInt());
        //int flag
        cmd.setFlag(headerBuffer.getInt());
        //String remark 先获取remark的长度
        int remarkLen = headerBuffer.getInt();
        if (remarkLen > 0) {
            byte[] remarkContent = new byte[remarkLen];
            headerBuffer.get(remarkContent);
            cmd.setRemark(new String(remarkContent, CHARSET_URT8));
        }
        //HashMap<String,String> extFields
        int extFieldsLength = headerBuffer.getInt();
        if (extFieldsLength > 0) {
            byte[] extFieldsBytes = new byte[extFieldsLength];
            headerBuffer.get(extFieldsBytes);
            //将字节反序列化为map
            cmd.setExtFields(mapDeserialize(extFieldsBytes));

        }

        return cmd;

    }


    /**
     * 字节数据反序列化为map
     *
     * @param bytes
     * @return
     */
    private static HashMap<String, String> mapDeserialize(byte[] bytes) {
        //先读量给字节作为key的长度，然后读取 key的值，然后读取4个字节作为value的长度，然后读取value的值
        if (bytes == null || bytes.length <= 0) {
            return null;
        }
        HashMap<String, String> map = new HashMap<>();
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        short keySize;
        byte[] keyContent;
        int valueSize;
        byte[] valueContent;
        while (byteBuffer.hasRemaining()) {
            keySize = byteBuffer.getShort();
            keyContent = new byte[keySize];
            byteBuffer.get(keyContent);

            valueSize = byteBuffer.getInt();
            valueContent = new byte[valueSize];
            byteBuffer.get(valueContent);
            map.put(new String(keyContent, CHARSET_URT8), new String(valueContent, CHARSET_URT8));
        }
        return map;

    }
}
