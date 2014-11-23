
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.*;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

public class main {

	static AWSCredentials credentials = null;
	static AmazonElasticMapReduce client =null;
	
	public static void main(String[] args) throws IOException {
		
		try {
			// AWS Authentication 
			credentials = new ProfileCredentialsProvider("Vaibhav")
			.getCredentials();
		} catch (Exception e1) {
		    System.out.println(e1.getMessage());
		    System.exit(-1);
		}
		
		//Creating EMR client and set region
		client = new AmazonElasticMapReduceClient(credentials);
		//client.setEndpoint("eu-west-1.elasticmapreduce.amazonaws.com");
		
		String clusterId = createCluster();
		System.out.println("Newly cluster created Id is " + clusterId);	 
		
		insertJob(clusterId);
	}
	
	public static String createCluster() throws IOException
	{
		StepFactory stepFactory = new StepFactory();

		StepConfig enabledebugging = new StepConfig()
		       .withName("Enable debugging")
		       .withActionOnFailure("TERMINATE_JOB_FLOW")
		       .withHadoopJarStep(stepFactory.newEnableDebuggingStep());
		
		StepConfig installHive = new StepConfig()
	       .withName("Install Hive")
	       .withActionOnFailure("TERMINATE_JOB_FLOW")
	       .withHadoopJarStep(stepFactory.newInstallHiveStep());
		
		InputStreamReader istream = new InputStreamReader(System.in) ;
		BufferedReader bufRead = new BufferedReader(istream) ;
		
		System.out.println("Please enter Name of the Cluster:");
		String userInputName = bufRead.readLine();
		
	    RunJobFlowRequest request1 = new RunJobFlowRequest()
	       .withName(userInputName)
	       .withSteps(enabledebugging, installHive)
	       .withLogUri("s3://cloudigratesample/")
	       .withInstances(new JobFlowInstancesConfig()
	           .withHadoopVersion("0.20.205")
	           .withInstanceCount(1)
	           .withKeepJobFlowAliveWhenNoSteps(false)
	           .withMasterInstanceType("m1.small")
	           .withSlaveInstanceType("m1.small"));
	    
	    
	    RunJobFlowResult result = client.runJobFlow(request1);
	    
	    return result.getJobFlowId();
	}
	
	public static void insertJob(String clusterId)
	{
	    // predefined steps. See StepFactory for list of predefined steps
		StepConfig hive = new StepConfig("Hive", new StepFactory().newInstallHiveStep());
				   
	    // A custom step
	    HadoopJarStepConfig hadoopConfig1 = new HadoopJarStepConfig()
	        .withJar("s3://cloudigratesample/wordcount.jar")
	        .withMainClass("WordCount") // optional main class, this can be omitted if jar above has a manifest
	        .withArgs("--verbose"); // optional list of arguments
	    StepConfig customStep = new StepConfig("Step1", hadoopConfig1);
	    AddJobFlowStepsResult result = client.addJobFlowSteps(new AddJobFlowStepsRequest()
	        .withJobFlowId(clusterId)
	       .withSteps(hive, customStep));
	    
	    System.out.println("Printing stpes Ids for sample program:");
	    System.out.println(result.getStepIds());

	}
}