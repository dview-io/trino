/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.dview.udfs;

import io.airlift.slice.Slice;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.security.spec.KeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.StandardTypes.VARCHAR;

public final class EncryptDecrypt
{
    private static final String AES256_KEY = System.getenv("AES_KEY");
    private static final String AES256_SALT = System.getenv("AES_SALT");
    private static final byte[] SALT = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    private static Cipher enCipher;
    private static Cipher deCipher;

    private EncryptDecrypt() {}

    @Description("UDF to encrypt a value")
    @ScalarFunction("encrypt")
    @SqlType(VARCHAR)
    public static Slice encrypt(
                @SqlNullable @SqlType(VARCHAR) Slice value)
    {
        return utf8Slice(EncryptDecrypt.encrypt(Objects.isNull(value) ? "" : value.toStringUtf8()));
    }

    @Description("UDF to decrypt a value")
    @ScalarFunction("decrypt")
    @SqlType(VARCHAR)
    public static Slice decrypt(ConnectorSession session, @SqlNullable @SqlType(VARCHAR) Slice value)
    {
        if (Arrays.asList(System.getenv("ADMIN_USERS").split(",")).contains(session.getUser())) {
            return utf8Slice(EncryptDecrypt.decrypt(Objects.isNull(value) ? "" : value.toStringUtf8()));
        }
        return utf8Slice(session.getUser() + " is not authorised for Decryption");
    }

    private static synchronized String encrypt(String value)
    {
        try {
            if (Objects.isNull(enCipher)) {
                SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
                KeySpec spec = new PBEKeySpec(AES256_KEY.toCharArray(), AES256_SALT.getBytes(StandardCharsets.UTF_8), 65536, 256);
                SecretKeySpec secretKey = new SecretKeySpec(factory.generateSecret(spec).getEncoded(), "AES");
                Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
                cipher.init(Cipher.ENCRYPT_MODE, secretKey, new IvParameterSpec(SALT));
                enCipher = cipher;
            }
            return Base64.getEncoder().encodeToString(enCipher.doFinal(value.getBytes(StandardCharsets.UTF_8)));
        }
        catch (Exception exception) {
            return exception.getMessage();
        }
    }

    private static synchronized String decrypt(String value)
    {
        try {
            if (Objects.isNull(deCipher)) {
                SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
                KeySpec spec = new PBEKeySpec(AES256_KEY.toCharArray(), AES256_SALT.getBytes(StandardCharsets.UTF_8), 65536, 256);
                SecretKeySpec secretKey = new SecretKeySpec(factory.generateSecret(spec).getEncoded(), "AES");
                Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
                cipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(SALT));
                deCipher = cipher;
            }
            return new String(deCipher.doFinal(Base64.getDecoder().decode(value)), StandardCharsets.UTF_8);
        }
        catch (Exception exception) {
            return "Wrong password for decryption";
        }
    }
}
