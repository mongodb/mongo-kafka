/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.kafka.connect.util;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.types.Password;

public class SslConfigs {
    
    private static final String EMPTY_STRING = "";
    
    public static final String CONNECTION_SSL_TRUSTSTORE_CONFIG = "connection.ssl.truststore";
    private static final String CONNECTION_SSL_TRUSTSTORE_DOC =
        "A trust store certificate location to be used for SSL enabled connections";
    public static final String CONNECTION_SSL_TRUSTSTORE_DEFAULT = EMPTY_STRING;
    private static final String CONNECTION_SSL_TRUSTSTORE_DISPLAY = "SSL TrustStore";

    public static final String CONNECTION_SSL_TRUSTSTORE_PASSWORD_CONFIG = "connection.ssl.truststorePassword";
    private static final String CONNECTION_SSL_TRUSTSTORE_PASSWORD_DOC =
        "A trust store password to be used for SSL enabled connections";
    public static final String CONNECTION_SSL_TRUSTSTORE_PASSWORD_DEFAULT = EMPTY_STRING;
    private static final String CONNECTION_SSL_TRUSTSTORE_PASSWORD_DISPLAY = "SSL TrustStore Password";

    public static final String CONNECTION_SSL_KEYSTORE_CONFIG = "connection.ssl.keystore";
    private static final String CONNECTION_SSL_KEYSTORE_DOC =
        "A key store certificate location to be used for SSL enabled connections";
    public static final String CONNECTION_SSL_KEYSTORE_DEFAULT = EMPTY_STRING;
    private static final String CONNECTION_SSL_KEYSTORE_DISPLAY = "SSL KeyStore";
    
    public static final String CONNECTION_SSL_KEYSTORE_PASSWORD_CONFIG = "connection.ssl.keystorePassword";
    private static final String CONNECTION_SSL_KEYSTORE_PASSWORD_DOC =
        "A key store password to be used for SSL enabled connections";
    public static final String CONNECTION_SSL_KEYSTORE_PASSWORD_DEFAULT = EMPTY_STRING;
    private static final String CONNECTION_SSL_KEYSTORE_PASSWORD_DISPLAY = "SSL KeyStore Password";
    
    public static ConfigDef addSslConfigDef(final ConfigDef configDef) {
        
        String group = "SSL";
        int orderInGroup = 0;
        configDef
            .define(
                    CONNECTION_SSL_TRUSTSTORE_CONFIG,
                    Type.STRING,
                    CONNECTION_SSL_TRUSTSTORE_DEFAULT,
                    Importance.MEDIUM,
                    CONNECTION_SSL_TRUSTSTORE_DOC,
                    group,
                    ++orderInGroup,
                    Width.LONG,
                    CONNECTION_SSL_TRUSTSTORE_DISPLAY)
            .define(
                    CONNECTION_SSL_TRUSTSTORE_PASSWORD_CONFIG,
                    Type.PASSWORD,
                    CONNECTION_SSL_TRUSTSTORE_PASSWORD_DEFAULT,
                    Importance.MEDIUM,
                    CONNECTION_SSL_TRUSTSTORE_PASSWORD_DOC,
                    group,
                    ++orderInGroup,
                    Width.MEDIUM,
                    CONNECTION_SSL_TRUSTSTORE_PASSWORD_DISPLAY)
            .define(
                    CONNECTION_SSL_KEYSTORE_CONFIG,
                    Type.STRING,
                    CONNECTION_SSL_KEYSTORE_DEFAULT,
                    Importance.MEDIUM,
                    CONNECTION_SSL_KEYSTORE_DOC,
                    group,
                    ++orderInGroup,
                    Width.LONG,
                    CONNECTION_SSL_KEYSTORE_DISPLAY)
            .define(
                    CONNECTION_SSL_KEYSTORE_PASSWORD_CONFIG,
                    Type.PASSWORD,
                    CONNECTION_SSL_KEYSTORE_PASSWORD_DEFAULT,
                    Importance.MEDIUM,
                    CONNECTION_SSL_KEYSTORE_PASSWORD_DOC,
                    group,
                    ++orderInGroup,
                    Width.MEDIUM,
                    CONNECTION_SSL_KEYSTORE_PASSWORD_DISPLAY)
            ;
        return configDef;
    }
    
    /**
     * Set key store and trust store parameters as system properties.
     * This will programmatically implement the following action in the command line before executing Java process: <br>
     * <pre>
     * # > export KAFKA_OPTS="\
     *         -Djavax.net.ssl.trustStore=<your path to truststore> \
     *         -Djavax.net.ssl.trustStorePassword=<your truststore password> \
     *         -Djavax.net.ssl.keyStore=<your path to keystore> \
     *         -Djavax.net.ssl.keyStorePassword=<your keystore password>"
     * </pre>
     * 
     * If set, default SslContext uses these parameters from Java System properties to establish SSL-enabled connection.        
     * 
     * @param connectorConfig - Sink our Source Connector properties with key/trust store parameters
     */
    public static void setupSsl(AbstractConfig connectorConfig){
        
        String val = connectorConfig.getString(SslConfigs.CONNECTION_SSL_TRUSTSTORE_CONFIG);
        if (val != null && !val.isBlank()) {
            System.setProperty("javax.net.ssl.trustStore", val);
        }
        
        Password passwordField = connectorConfig.getPassword(SslConfigs.CONNECTION_SSL_TRUSTSTORE_PASSWORD_CONFIG);
        if (passwordField != null && !(val=passwordField.value()).isBlank()) {
            System.setProperty("javax.net.ssl.trustStorePassword", val);
        }

        val = connectorConfig.getString(SslConfigs.CONNECTION_SSL_KEYSTORE_CONFIG);
        if (val != null && !val.isBlank()) {
            System.setProperty("javax.net.ssl.keyStore", val);
        }
        
        passwordField = connectorConfig.getPassword(SslConfigs.CONNECTION_SSL_KEYSTORE_PASSWORD_CONFIG);
        if (passwordField != null && !(val=passwordField.value()).isBlank()) {
            System.setProperty("javax.net.ssl.keyStorePassword", val);
        }
    }
}
