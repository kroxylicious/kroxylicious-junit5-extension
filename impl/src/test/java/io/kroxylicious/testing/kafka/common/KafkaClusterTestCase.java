package io.kroxylicious.testing.kafka.common;

public class KafkaClusterTestCase {
    private String displayName;
    private int brokersNum;
    private boolean kraftMode;
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

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }
    public int getBrokersNum() {
        return brokersNum;
    }

    public void setBrokersNum(int brokersNum) {
        this.brokersNum = brokersNum;
    }

    public boolean isKraftMode() {
        return kraftMode;
    }

    public void setKraftMode(boolean kraftMode) {
        this.kraftMode = kraftMode;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
