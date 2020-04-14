package net.dempsy;

import java.util.function.Function;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.threading.ThreadingModel;
import net.dempsy.util.SystemPropertyManager;

@Ignore
public class TestWordCountLimitedContainerQueue extends TestWordCount {
    private static Logger LOGGER = LoggerFactory.getLogger(TestWordCountLimitedContainerQueue.class);

    public static SystemPropertyManager props;

    @SuppressWarnings("resource")
    @BeforeClass
    public static void setupClass() {
        props = new SystemPropertyManager().set("max-pending-messages-per-ontainer", "0");
    }

    @AfterClass
    public static void cleanup() {
        props.close();
    }

    public TestWordCountLimitedContainerQueue(final String routerId, final String containerId, final String sessCtx, final String tpid, final String serType,
        final String threadingModelDescription, final Function<String, ThreadingModel> threadingModelSource) {
        super(LOGGER, routerId, containerId, sessCtx, tpid, serType, threadingModelDescription, threadingModelSource, true);
        strict = false;
    }
}
