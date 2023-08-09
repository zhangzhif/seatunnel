package transform;

import org.apache.seatunnel.transform.crypto.AesDecrypt;
import org.apache.seatunnel.transform.crypto.AesEncrypt;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class AesTest {

    @Test
    public void testEncrypt() throws Exception {
        AesEncrypt encrypt = new AesEncrypt();
        System.out.println(encrypt.evaluate(Arrays.asList("ocean")));
        AesDecrypt decrypt = new AesDecrypt();
        System.out.println(decrypt.evaluate(Arrays.asList("6dc3353405f8c38cf5e4bfddecd10653")));
    }
}
