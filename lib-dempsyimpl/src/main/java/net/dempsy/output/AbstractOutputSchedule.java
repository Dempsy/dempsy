package net.dempsy.output;

import java.util.Map;

import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.output.OutputExecuter;
import net.dempsy.output.OutputInvoker;

public abstract class AbstractOutputSchedule implements OutputExecuter
{
   /** The logger. */
   protected static Logger logger = LoggerFactory.getLogger(AbstractOutputSchedule.class);
   
   /** Contains the number of threads to set on the {@link OutputInvoker} */
   protected int concurrency = -1;

   /** The output invoker. */
   protected OutputInvoker outputInvoker;
   
   /** The scheduler. */
   protected Scheduler scheduler;
   
   /** The OUTPUT JOB NAME. */
   public static String OUTPUT_JOB_NAME = "outputInvoker";

   /**
    * Gets the job detail.
    *
    * @param outputInvoker the output invoker
    * @return the job detail
    */
   protected JobDetail getJobDetail() {
     JobBuilder jobBuilder = JobBuilder.newJob(OutputJob.class);
     JobDetail jobDetail = jobBuilder.build();
     jobDetail.getJobDataMap().put(OUTPUT_JOB_NAME, this);
     return jobDetail;
   }

   public int getConcurrency() { return concurrency; }

   public void setConcurrency(int concurrency) { this.concurrency = concurrency; }

   /* (non-Javadoc)
    * @see com.nokia.dempsy.output.OutputExecuter#setOutputInvoker(com.nokia.dempsy.output.OutputInvoker)
    */
   @Override
   public void setOutputInvoker(OutputInvoker outputInvoker) {
     this.outputInvoker = outputInvoker;

     if (concurrency > 1)
        outputInvoker.setConcurrency(concurrency);
   }
   
   /**
    * Container will invoke this method.
    */
   @Override
   public void stop() {
     try {
       // gracefully shutting down
        if (scheduler != null)
           scheduler.shutdown();
     } catch (SchedulerException se) {
       logger.error("Error occurred while stopping the relative scheduler : " + se.getMessage(), se);
     }
   }
   
   protected void doInvoke()
   {
      this.outputInvoker.invokeOutput();
   }
   
   /**
    * The Class OutputJob. This class is responsible to call invokeOutput() method.
    * This class is being called by Quartz scheduler.
    */
   public static class OutputJob implements Job {
     public void execute(JobExecutionContext context) throws JobExecutionException {

       @SuppressWarnings("unchecked")
       Map<String, AbstractOutputSchedule> dataMap = (Map<String, AbstractOutputSchedule>)context.getJobDetail().getJobDataMap();
       AbstractOutputSchedule outputInvoker = dataMap.get(OUTPUT_JOB_NAME);

       if (outputInvoker != null) {
         // execute MP's output method
         outputInvoker.doInvoke();
       } else {
         logger.warn("outputInvoker is NULL");
       }
     }
   }
}
