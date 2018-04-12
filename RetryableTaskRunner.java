import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RetryableTaskRunner {

    private static final Log log = LogFactory.getLog(RetryableTaskRunner.class);

    public static final int DEFAULT_NUM_ATTEMPTS = 10;
    public static final int DEFAULT_SLEEP_INTERVAL = 2;

    private int numAttempts, sleepInterval;

    public RetryableTaskRunner() {
        this(DEFAULT_NUM_ATTEMPTS, DEFAULT_SLEEP_INTERVAL);
    }

    public RetryableTaskRunner(int numAttempts, int sleepIntervalInSeconds) {
        this.numAttempts = numAttempts;
        this.sleepInterval = sleepIntervalInSeconds;
    }

    public void run(RetryableTask task) throws Exception {
        for (int i=0;i<=numAttempts;i++) {
            try {
                task.run();
                return;
            }
            catch (Exception e) {
                if (i==numAttempts) {
                    log.error("Retry Error", e);
                    throw e;
                }
                try {
                    Thread.sleep(sleepInterval * 1000);
                }
                catch (InterruptedException ie) {}
                log.error("Got an exception! Retry for the "+(i+1)+"/"+numAttempts+" times.", e);
            }
        }
    }

}
