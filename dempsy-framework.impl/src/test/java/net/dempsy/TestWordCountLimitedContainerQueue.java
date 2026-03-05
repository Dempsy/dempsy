package net.dempsy;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.util.SystemPropertyManager;

@Disabled
public class TestWordCountLimitedContainerQueue extends TestWordCount {
    private static Logger LOGGER = LoggerFactory.getLogger(TestWordCountLimitedContainerQueue.class);

    {
        ((DempsyBaseTest)this).LOGGER = TestWordCountLimitedContainerQueue.LOGGER;
        this.strict = false;
        this.considerOnlyContainersThatCanLimit = true;
    }

    public static SystemPropertyManager props;

    @SuppressWarnings("resource")
    @BeforeAll
    public static void setupClass() {
        props = new SystemPropertyManager().set("max-pending-messages-per-ontainer", "0");
    }

    @AfterAll
    public static void cleanup() {
        props.close();
    }
}
