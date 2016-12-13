package net.dempsy.output;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import net.dempsy.container.MpContainer;
import net.dempsy.output.CronOutputSchedule;
import net.dempsy.output.RelativeOutputSchedule;

/**
 * The Class OutputScheduleTest.
 */
public class OutputScheduleTest {
  
  /** The mp container mock. */
  @Mock
  MpContainer mpContainerMock;
  
  /**
   * Sets the up.
   *
   * @throws Exception the exception
   */
  @Before
  public void setUp() throws Exception {
    //initializing
    MockitoAnnotations.initMocks(this);
  
   
  }
  
  /**
   * Test relative schedule.
   *
   * @throws Exception the exception
   */
  @Test
  public void testRelativeSchedule() throws Exception {
    RelativeOutputSchedule relativeOutputSchedule = new RelativeOutputSchedule(1, TimeUnit.SECONDS);
    relativeOutputSchedule.setOutputInvoker(mpContainerMock);
    try
    {
       relativeOutputSchedule.start();
       Thread.sleep(1000);
       verify(mpContainerMock, atLeast(1)).invokeOutput();
    }
    finally
    {
       relativeOutputSchedule.stop();
    }
  }

  /**
   * Test relative schedule with concurrency.
   *
   * @throws Exception the exception
   */
  @Test
  public void testRelativeScheduleWithConcurrency() throws Exception {
    RelativeOutputSchedule relativeOutputSchedule = new RelativeOutputSchedule(1, TimeUnit.SECONDS);
    relativeOutputSchedule.setConcurrency(5);
    relativeOutputSchedule.setOutputInvoker(mpContainerMock);
    try
    {
       relativeOutputSchedule.start();
       Thread.sleep(1000);
       verify(mpContainerMock, atLeast(1)).invokeOutput();
       verify(mpContainerMock, atLeast(1)).setConcurrency(5);
    }
    finally
    {
       relativeOutputSchedule.stop();
    }
  }

  /**
   * Test cron schedule.
   *
   * @throws Exception the exception
   */
  @Test
  public void testCronSchedule() throws Exception {
    CronOutputSchedule cronOutputSchedule = new CronOutputSchedule("0/1 * * * * ?");
    cronOutputSchedule.setOutputInvoker(mpContainerMock);
    try
    {
       cronOutputSchedule.start();
       Thread.sleep(1000);
       verify(mpContainerMock, atLeast(1)).invokeOutput();
    }
    finally
    {
       cronOutputSchedule.stop();
    }
  }
  
  /**
   * Test cron schedule with concurrency setting
   *
   * @throws Exception the exception
   */
  @Test
  public void testCronScheduleWithConcurrencySetting() throws Exception {
    CronOutputSchedule cronOutputSchedule = new CronOutputSchedule("0/1 * * * * ?");
    cronOutputSchedule.setConcurrency(5);
    cronOutputSchedule.setOutputInvoker(mpContainerMock);
    try
    {
       cronOutputSchedule.start();
       Thread.sleep(1000);
       verify(mpContainerMock, atLeast(1)).invokeOutput();
       verify(mpContainerMock, atLeast(1)).setConcurrency(5);
    }
    finally
    {
       cronOutputSchedule.stop();
    }
  }

}
