#!/usr/bin/perl -w
use strict;
use TaskManager2::ManagerFactory;

# 1 - Create the manager: workflow name and logs path
my $runner =  TaskManager2::ManagerFactory -> new("WorkflowName", "./test");

# 2 - Set maximum number of executable tasks in parallel
# WARNING: a task can use multiple cores
$runner -> setMaxProc(120); # max tasks submit per user

# 3 - Create the tasks
#my $jobA = $runner -> createJob("NomDuJobA", "sleep 100"); # construction du job A
#my $jobB = $runner -> createJob("NomDuJobB", "sleep 100"); # construction du job B

# 4 - Specify a dependency-
#$jobB -> addDep($jobA);
my $cmp = 1;
while($cmp < 2){
	$runner -> createJob("NomDuJobA".$cmp, "sleep 15");
	#$runner -> createJob("NomDuJobB".$cmp, "sleep 15");
	#$runner -> createJob("NomDuJobC".$cmp, "sleep 15");
	#$runner -> createJob("NomDuJobD".$cmp, "sleep 15");
	$cmp++;
}

$runner -> run();  

    
    
    
    