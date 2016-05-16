import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigInteger;

import java.io.UnsupportedEncodingException;
import sun.misc.*;


public class Code
{
    private static char map1[];
    private static byte map2[];

    private Code()
    {

    }
    public static void main (String[] args) throws java.lang.Exception
    {
        if(args.length==0)
            System.out.println("Error: no input text in arguments");
        else if(args.length==1)
        {
            String str = args[0];
            String result = encrype(str);
            System.out.println("[Text]: " + str);
            System.out.println("[OK]: [" + result + "]");
        }
        else{
            String efs;
            efs = "seg2y1/rP7CumyEJuxUVkEGfahzp7n7YFxMIC5a+fQ5GeZDU/geH6vNYRDBzGJ5Z8KpEbxeIc8Qo/b259Bv2nzNJsxn/Y2fMVgVOZf1j4Vdia5koGEbT8syQ59NvO7tA";
            System.out.println("This is test ..");
            System.out.println("[1]result - efs: " + efs);
            System.out.println("[1]result - decrype: " + decrype(efs));
            System.out.println("[1]result - encrype: " + encrype(decrype(efs)));
            System.out.println("");
            efs = "seg2y1/rP7CumyEJuxUVkC9dhrq8KC0McHFWD9FKQoD1RnQslSnPZiTZqqzJpebHdU3f33wPBWvuhgrT12OLgQ==";
            System.out.println("[2]result - efs: " + efs);
            System.out.println("[2]result - decrype: " + decrype(efs));
            System.out.println("[2]result - encrype: " + encrype(decrype(efs)));
            System.out.println("");
            System.out.println("===============sign==================");
            String sb = new StringBuffer("==gNOxmWKFUIuVDQ9U3OTZlM8siLqV2QsZkT").reverse().toString();
            System.out.println("---- secret: " + new String(decode(sb)));
            System.out.println("---- try HmacSHA1:" + "123");
            System.out.println("---- result 1:" +  mac_encrype("123"));
            System.out.println("---- result 2:" +  mac_encrype("123"));
            System.out.println("---- result 3:" +  mac_encrype("123"));
        }
    }
    /*AES 解密*/
    public static String decrype(String str)throws java.lang.Exception
    {
        Cipher localCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        String secret = "3f2c593b7d469602af5a6fb718bc92cc";
        SecretKeySpec localSecretKeySpec = new SecretKeySpec(new BigInteger(secret, 16).toByteArray(), "AES");
        IvParameterSpec localIvParameterSpec = new IvParameterSpec(new byte[16]);
        localCipher.init(Cipher.DECRYPT_MODE, localSecretKeySpec, localIvParameterSpec);
        return new String(localCipher.doFinal(decode(str)));
    }
    /*AES加密*/
    public static String encrype(String str)throws java.lang.Exception
    {
        Cipher localCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        String secret = "3f2c593b7d469602af5a6fb718bc92cc";
        SecretKeySpec localSecretKeySpec = new SecretKeySpec(new BigInteger(secret, 16).toByteArray(), "AES");
        IvParameterSpec localIvParameterSpec = new IvParameterSpec(new byte[16]);
        localCipher.init(Cipher.ENCRYPT_MODE, localSecretKeySpec, localIvParameterSpec);
        return String.valueOf(encode(localCipher.doFinal(str.getBytes())));
        // return new String(encode(localCipher.doFinal(str.getBytes())));
    }


    /*HmacSHA1加密*/
    public static String mac_encrype(String str)throws java.lang.Exception
    {
        String sb = new StringBuffer("==gNOxmWKFUIuVDQ9U3OTZlM8siLqV2QsZkT").reverse().toString();
        Mac localMac = Mac.getInstance("HmacSHA1");
        localMac.init(new SecretKeySpec(decode(sb), "HmacSHA1"));
        return String.valueOf(encode(localMac.doFinal(str.getBytes())));
    }

    public static String decodeString(String s)
    {   return new String(decode(s));
    }
    public static String encodeString(String s)
    {
        return new String(encode(s.getBytes()));
    }
    public static byte[] decode(String s)
    {
        return decode(s.toCharArray());
    }

    public static byte[] decode(char ac[])
    {
        int i = ac.length;
        if(i % 4 != 0)
            throw new IllegalArgumentException("Length of Base64 encoded input string is not a multiple of 4.");
        for(; i > 0 && ac[i - 1] == '='; i--);
        int j = (i * 3) / 4;
        byte abyte0[] = new byte[j];
        int k = 0;
        int l = 0;
        while(l < i)
        {
            int i1 = l + 1;
            char c = ac[l];
            int j1 = i1 + 1;
            char c1 = ac[i1];
            char c2;
            char c3;
            int k1;
            if(j1 < i)
            {
                int j3 = j1 + 1;
                c2 = ac[j1];
                j1 = j3;
            } else
            {
                c2 = 'A';
            }
            if(j1 < i)
            {
                k1 = j1 + 1;
                c3 = ac[j1];
            } else
            {
                c3 = 'A';
                k1 = j1;
            }
            if(c > '\177' || c1 > '\177' || c2 > '\177' || c3 > '\177')
                throw new IllegalArgumentException("Illegal character in Base64 encoded data.");
            byte byte0 = map2[c];
            byte byte1 = map2[c1];
            byte byte2 = map2[c2];
            byte byte3 = map2[c3];
            if(byte0 < 0 || byte1 < 0 || byte2 < 0 || byte3 < 0)
                throw new IllegalArgumentException("Illegal character in Base64 encoded data.");
            int l1 = byte0 << 2 | byte1 >>> 4;
            int i2 = (byte1 & 0xf) << 4 | byte2 >>> 2;
            int j2 = byte3 | (byte2 & 3) << 6;
            int k2 = k + 1;
            abyte0[k] = (byte)l1;
            int l2;
            int i3;
            if(k2 < j)
            {
                l2 = k2 + 1;
                abyte0[k2] = (byte)i2;
            } else
            {
                l2 = k2;
            }
            if(l2 < j)
            {
                i3 = l2 + 1;
                abyte0[l2] = (byte)j2;
            } else
            {
                i3 = l2;
            }
            k = i3;
            l = k1;
        }
        return abyte0;
    }



    public static char[] encode(byte abyte0[])
    {
        return encode(abyte0, abyte0.length);
    }

    public static char[] encode(byte abyte0[], int i)
    {
        int j = (2 + i * 4) / 3;
        char ac[] = new char[4 * ((i + 2) / 3)];
        int k = 0;
        int l = 0;
        while(l < i)
        {
            int i1 = l + 1;
            int j1 = 0xff & abyte0[l];
            int k1;
            int l1;
            int i2;
            int j2;
            int k2;
            int l2;
            int i3;
            int j3;
            int k3;
            int l3;
            char c;
            int i4;
            char c1;
            if(i1 < i)
            {
                k1 = i1 + 1;
                l1 = 0xff & abyte0[i1];
            } else
            {
                k1 = i1;
                l1 = 0;
            }
            if(k1 < i)
            {
                i2 = k1 + 1;
                j2 = 0xff & abyte0[k1];
            } else
            {
                i2 = k1;
                j2 = 0;
            }
            k2 = j1 >>> 2;
            l2 = (j1 & 3) << 4 | l1 >>> 4;
            i3 = (l1 & 0xf) << 2 | j2 >>> 6;
            j3 = j2 & 0x3f;
            k3 = k + 1;
            ac[k] = map1[k2];
            l3 = k3 + 1;
            ac[k3] = map1[l2];
            if(l3 < j)
                c = map1[i3];
            else
                c = '=';
            ac[l3] = c;
            i4 = l3 + 1;
            if(i4 < j)
                c1 = map1[j3];
            else
                c1 = '=';
            ac[i4] = c1;
            k = i4 + 1;
            l = i2;
        }
        return ac;
    }


    static
    {
        map1 = new char[64];
        char c = 'A';
        int i;
        int k1;
        for(i = 0; c <= 'Z'; i = k1)
        {
            char ac4[] = map1;
            k1 = i + 1;
            ac4[i] = c;
            c++;
        }

        for(char c1 = 'a'; c1 <= 'z';)
        {
            char ac3[] = map1;
            int j1 = i + 1;
            ac3[i] = c1;
            c1++;
            i = j1;
        }

        for(char c2 = '0'; c2 <= '9';)
        {
            char ac2[] = map1;
            int i1 = i + 1;
            ac2[i] = c2;
            c2++;
            i = i1;
        }

        char ac[] = map1;
        int j = i + 1;
        ac[i] = '+';
        char ac1[] = map1;
        int _tmp = j + 1;
        ac1[j] = '/';
        map2 = new byte[128];
        for(int k = 0; k < map2.length; k++)
            map2[k] = -1;

        for(int l = 0; l < 64; l++)
            map2[map1[l]] = (byte)l;

    }

}
