package com.nokia.dempsy.output;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.nokia.dempsy.container.MpContainer;

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
    relativeOutputSchedule.start();
    Thread.sleep(1000);
    verify(mpContainerMock, atLeast(1)).invokeOutput();
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
    cronOutputSchedule.start();
    Thread.sleep(1000);
    verify(mpContainerMock, atLeast(1)).invokeOutput();
  }
}
