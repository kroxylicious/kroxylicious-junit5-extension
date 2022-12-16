package io.kroxylicious.testing.kafka.common;

public class KafkaClusterTestCase {
    private final String displayName;
    private final int brokersNum;
    private final boolean kraftMode;
    private String version;

    public KafkaClusterTestCase(String displayName, int brokersNum, boolean kraftMode, String version) {
        this.displayName = displayName;
        this.brokersNum = brokersNum;
        this.kraftMode = kraftMode;
        this.version = version;
    }

    public String getDisplayName() {
        return displayName;
    }

    public int getBrokersNum() {
        return brokersNum;
    }

    public boolean isKraftMode() {
        return kraftMode;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
