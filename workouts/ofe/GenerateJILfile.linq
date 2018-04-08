<Query Kind="Program">
  <Reference Relative="..\CSharpLibs\NUnit-2.6.3\bin\framework\nunit.framework.dll">E:\Dev\CSharpLibs\NUnit-2.6.3\bin\framework\nunit.framework.dll</Reference>
  <Reference>&lt;RuntimeDirectory&gt;\WPF\PresentationFramework.dll</Reference>
  <Reference>&lt;RuntimeDirectory&gt;\System.Runtime.dll</Reference>
  <NuGetReference>Rx-Main</NuGetReference>
  <Namespace>System.Reactive.Linq</Namespace>
  <Namespace>System.Reactive.Subjects</Namespace>
</Query>

void Main()
{
	var apps = new [] {"871m_cplf_microservice", "871m_eqrms-data-microservice","871m_orchestration-microservice"};
	var startstop = new [] {"START", "STOP" };
	var prodCob = new [] {"-PROD", "-COB" };
	foreach (var entity in new Entity().GetEntities())	
		foreach(var app in apps)
			foreach(var machine in entity.Machines)
			 	foreach(var prodC in prodCob)
			 		foreach(var startStp in startstop)
						print(entity.CsiId, app, startStp,machine, entity.OwnerFid, entity.Region+prodC, "x:\\eqdpt\\reg871m", app+".jil");	
}

// Define other methods and classes here
void print(int csiId, string appName, string startOrStop, string machine, string fid, string profile, string appRoot, string fileName)
{
	Console.WriteLine (getTemplate(), csiId, appName, startOrStop, machine, fid, profile, appRoot);
}

string getTemplate()
{
	StringBuilder jilFile = new StringBuilder("/*---------------------{0}_{1}_{2}---------------------*/\n");
	jilFile.Append("insert_job: {0}_{1}_{2}_job           job_type:cmd\n");
	jilFile.Append("command: {6}\\{1}\\current\\resources\\scripts\\sc.bat {1} {5} {2} {6}\n");
	jilFile.Append("description: {2} {1} owned by TradeCapture team\n");
	jilFile.Append("machine: {3}\n");
	jilFile.Append("Owner: {4}\n");
	jilFile.Append("max_run_alarm: 5\n");
	jilFile.Append("alarm_if_fail: y\n");	
	jilFile.Append("send_notification: n\n");
	jilFile.Append("std_out_file: C:\\temp\\{1}_out.log\n"); 
	jilFile.Append("std_err_file: C:\\temp\\{1}_err.log\n");
	return jilFile.ToString();
	
}

public class Entity 
{
	public string Region;
	public int CsiId;
	public string OwnerFid;
	public string[] Machines;
	
	public Entity[] GetEntities()
	{
		return new Entity[] 
		{
			new Entity{ Region="APAC", CsiId=1, OwnerFid="sgibesc@APAC",  Machines=new [] {"xd-askdf-1000","xd-askdf-1001","xd-askdf-1002"}},
			new Entity{ Region="EMEA", CsiId=2, OwnerFid="eqstped@EMEA",  Machines=new [] {"zx-askdf-2000","xd-askdf-2001","xd-askdf-2002"}},
			new Entity{ Region="NAM", CsiId=3, OwnerFid="eqstpbat01@NAM",  Machines=new []{"xd-askdf-3000","xd-askdf-3001","xd-askdf-3002"}}			
		};
	}
}