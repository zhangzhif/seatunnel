package transform;

import org.apache.seatunnel.transform.crypto.DesDecrypt;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class DesTest {

    @Test
    public void testEncrypt() throws Exception {
        DesDecrypt decrypt = new DesDecrypt();
        decrypt.evaluate(Arrays.asList("ocean"));
    }
}
