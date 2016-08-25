################################################################################
#
# * $Id: BatchManager.pm,v 0.1 2014/04/2 akourlai Exp $
#
# * BatchManager module for TaskManager2::BatchManager.pm
# *
# * Copyright Artem Kourlaiev / Institut de Genomique / DSV / CEA
# *                            <akourlai@genoscope.cns.fr>
# *
# * The purpose of this library, named TaskManager, is to provide an efficient
# * way to interact with computing-grid through SSH, LSF, or SLURM.
# * 
# *
# * This software is governed by the CeCILL license under French law and
# * abiding by the rules of distribution of free software.  You can  use,
# * modify and/ or redistribute the software under the terms of the CeCILL
# * license as circulated by CEA, CNRS and INRIA at the following URL
# * "http://www.cecill.info".
# *
# * As a counterpart to the access to the source code and  rights to copy,
# * modify and redistribute granted by the license, users are provided only
# * with a limited warranty  and the software's author,  the holder of the
# * economic rights,  and the successive licensors  have only  limited
# * liability.
# *
# * In this respect, the user's attention is drawn to the risks associated
# * with loading,  using,  modifying and/or developing or reproducing the
# * software by the user in light of its specific status of free software,
# * that may mean  that it is complicated to manipulate,  and  that  also
# * therefore means  that it is reserved for developers  and  experienced
# * professionals having in-depth computer knowledge. Users are therefore
# * encouraged to load and test the software's suitability as regards their
# * requirements in conditions enabling the security of their systems and/or
# * data to be ensured and,  more generally, to use and operate it in the
# * same conditions as regards security.
# *
# * The fact that you are presently reading this means that you have had
# * knowledge of the CeCILL license and that you accept its terms.
################################################################################
# POD documentation

=head1 NAME

TaskManager2::BatchManager.pm - Module permettant d'executer des taches liées par des contraintes avec batch manager en mode interactif.

=head1 SYNOPSIS
    
    Voir TaskManager2::ManagerFactory.

=head1 DESCRIPTION

TaskManager2::BatchManager Module permettant d'executer des taches liées par des contraintes avec batch manager en mode interactif.

=head1 AUTHOR

N'hésitez pas à me contacter pour tout renseignement, ajout de fonctionalité ou signalement de bugs.

Artem Kourlaiev, akourlai@genoscope.cns.fr

=cut

package TaskManager2::BatchManager;

use TaskManager2::Manager;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
our @ISA = qw(Exporter TaskManager2::Manager);
# Export par defaut
@EXPORT = qw();
# Export autorisé
@EXPORT_OK = qw();

$VERSION = '0.1';

use strict;
require Exporter;
use Scalar::Util 'refaddr';
use POSIX qw( :sys_wait_h getpid);
use Net::Domain qw(hostname);

our $BM_CMD = $TaskManager2::ManagerFactory::BM_CMD;
our $WORKFLOW_NAME = $TaskManager2::ManagerFactory::WORKFLOW_NAME;

my $DEBUG = 0;
my $JOB_FAIL = 0;

our @ALL_SUBMITED_TASKS = ();

sub interrupt (){
    print STDERR "Annulation du programme par l'utilisateur!\n";
    print STDERR "\n\n[TaskManager2::BatchManager] Les jobs soumis auparavant sont tues ...\n";
    
    foreach(@ALL_SUBMITED_TASKS){
       my $taskID = $_;
       my $output = `scancel $taskID 2>&1`;
       print STDERR "\tID : " .$taskID."\n";
    }
    exit 2;
}

$SIG{INT} = \&interrupt;

=head2 new
 Nom       : new
 Fonction  : Crée un objet de la classe TaskManager2::BatchManager.
 Exemple   : my $manager = new TaskManager2::BatchManager($name, $outDir);
 Retour    : une référence sur un objet de la classe TaskManager2::BatchManager.
 Arguments : 2 STRING, le nom du pipepline et le path où créer le dossier out, non obligatoire.

=cut
sub new {
    my $class = shift;
    my $self = $class -> SUPER::new(@_);
    bless( $self, $class);# lie la référence à la classe courante
    $self -> {CMP} = 0; # initialise le nombre de run à 0.
    return $self;
}

=head2 run

 Nom       : run
 Fonction  : Soumet les taches crées suivant l'ordre des dépendances. Le script est en pause tant que toutes les taches du Manager ne sont pas términées. A la fin du run, le Manager est purgé, il ne contient plus aucune tache. Le script peut continuer et lancer d'autres run.
 Exemple   : $manager -> run();
 Retour    : Null.
 Arguments : Null.

=cut
sub run {
    my $self = shift;
    
    if($self -> getNumberRun() == 0){
    	$self -> _make();
    }
    $self -> setNumberRun($self -> getNumberRun() + 1);# on incrémente le nombre de fois où un run du même Manager a été appelé.
    print "\n>>>>>>>>>>>>>>>>>\nSTART SUBMIT : ".$self -> getNumberRun()." - $WORKFLOW_NAME\n>>>>>>>>>>>>>>>>>\n" if $DEBUG;
    $self -> toManagerHistoryFile("<RUN number=\'".$self -> getNumberRun()."\' maxJobInParallel=\'".$self -> getMaxProc()."\'>\n");
    my $graph = $self -> _setGraph($self -> getTasks());
    
    ## RESTRUCTURE POUR GLOST ##
    if ($self -> reduceNumberOfJob() and int (@{$self -> getTasks()}) > $self -> getLimitToGlost()){ # activation de glost à partir de getLimitToGlost jobs dans le DAG.
    	$BM_CMD = "glost";
    	$TaskManager2::ManagerFactory::BM_CMD = "glost";
    	$TaskManager2::TaskFactory::BM_CMD = "glost";
    }
    
    if($BM_CMD eq "glost"){
        $graph = $self -> _toGlostTask($graph, $self -> getLimitToGlost()); # regrouppement de cmds dans un job lorsque plus de getLimitToGlost en //.
    }
    
    ## FIN RESTRUCTURE POUR GLOST ##
    
    while($graph -> vertices() and !$JOB_FAIL){ # tant qu'il y des noeuds dans le graph.
    
        my @sinkVertices = $graph -> sink_vertices();# récupere toutes les feuilles du graph (tache sans dep mais dont dépendent d'autres taches)
        my @isolatedVertices = $graph -> isolated_vertices();# récupere les noeuds isolés du graph, sans père ni fils (tache sans dep et dont aucune autre taches depend)         
        
        my @tasksToSubmit = (@sinkVertices, @isolatedVertices);
        
        if(@tasksToSubmit == 0){
        	die ("[".ref($self)."] Boucle dans le graphe de dependance !\n");
        }
        
        foreach(@tasksToSubmit){
           my $task = $_;
           while(!$self -> _checkLimitSubmitJob()){ ## On verifi le nombre de taches déjà soumises.
               sleep(20);
               print "Limite du nombre de jobs max (".$self -> getMaxSubmitJobPerUser().") soumis par UTILISATEUR est depassee, attente que les jobs deja soumis se terminent pour soumettre les jobs suivants...\n";
           }
           
           while(!$self -> _checkLimitSubmitJobForWorkflow() and !$JOB_FAIL){ ## On verifi si le nombre de jobs deja soumis n'est pas superieur à la limite de jobs en parallele pour le workflow.
              sleep(20);
              print "Limite du nombre de jobs max (".$self -> getMaxProc().") soumis par le WORKFLOW est depassee, attente que les jobs deja soumis se terminent pour soumettre les jobs suivants...\n";
           }
           
           $task -> setOutDir($self -> getOutDir());# affecte le outDir du Manager au outDir de la tache.
           $task -> make(); # crée le répertoire de la tache et les fichiers run.sh et history.xml.
           $task -> makeCmdToExecute();
           
           my $cmdToExecute = $task -> getCmdToExecute();
           my $outExecute = "";
           
           ### Traitement spécifique des taches non Slurm, executées en interactif
           if((ref($task) eq "TaskManager2::TaskLocal" || ref($task) eq "TaskManager2::TaskSsh" || ref($task) eq "TaskManager2::TaskGsissh" and !$JOB_FAIL)){
              
               while(int(@{$self -> getTasksWithStatus(0)}) > 0 and !$JOB_FAIL){
               		sleep (5);
               		$self -> setJobsInfos();
               }
               
               if(!$JOB_FAIL){
	               $task -> setID(POSIX::getpid());
	               print {$self -> getOutHandle()} $self -> toStringTaskInfos($task);
	               $task -> setBegin();
	               print "GO  ".$task -> getName()." ".localtime(time)." Host domaine : ".hostname()."\n";
	               $outExecute = `$cmdToExecute`;
	               print "END ".$task -> getName()." ".localtime(time)." Host domaine : ".hostname()."\n";
	               $task -> setEnd();
	               $task -> setReturn(WEXITSTATUS($?));
	               ############## Opération sur le fichier history.xml et allHistory.xml ##########
	               my $history = $_ -> historyFileToString();# on archive les logs des jobs COMPLETED et sans erreur dans le history de la tache et le allHistory du Manager 
	        	   $_ -> toTaskHistoryFile($history);
	        	   $self -> toManagerHistoryFile($history);
	        	   
	        	   if($task -> getReturn > 0){
	        	   		$JOB_FAIL = 1;
	        	   		$task -> setStatus(0);
	        	   }else{
	        	   		$task -> setStatus(1);
	        	   }
               }
               $graph -> delete_vertex($task); # supprime la tache soumise du graph
           }elsif(!$JOB_FAIL){
               $outExecute = `$cmdToExecute`;
               
               if($task -> extractID($outExecute) eq 1){  
                  print {$self -> getOutHandle()} $self -> toStringTaskInfos($task); # print dans le fichier infos.log
                  $task -> setStatus(0);
                  print "Submit job : (".$task -> getID().") ".$task-> getName()."\n";
                  # Memorisation de l'ID Slurm pour annulation des tâches en cas de Ctrl-C
                  push(@ALL_SUBMITED_TASKS, $task -> getID());
                  # Suppression de la taches soumise du graphe
                  $graph -> delete_vertex($task); # supprime la tache soumise du graph
               }else{
                   my $errorMessage = "";
                   $errorMessage .= $self -> toStringTaskInfos($task); 
                   # Si une soumission est rejeté par le Batch Manager
                   # IL FAUT SUPPRIMER TOUS LES JOBS EN EXECUTION AVANT DE MOURIR
                   $errorMessage .=  $self -> cancelAllSubmitedTask();
                   $errorMessage .= "\n[".ref($self)."] Message d'erreur de Slurm : ".$outExecute."\nFAIL SUBMISSION ".$self -> getNumberRun()."\n";
                   die ($errorMessage);
               }   
           }else{
           		my $run = $self -> _waiting_with_slurm_request();
           		
           		if($run eq ""){# attend que les jobs soumis se terminent pour rendre la main.
		        	print "DONE RUN ".$self -> getNumberRun()."\n";
		        }else{
		        	$run .= "FAIL RUN ".$self -> getNumberRun()."\n";
		        	die($run);
		        }
           }
        }
    }
    ######
    # Fin de la soumission du DAG de taches, attente que toutes les taches soient terminées pour rendre la main
    ######
    if(@{$self -> getTasks()} != 0){ # N'attend que si TaskManager contient des Taches.
    	my $run = $self -> _waiting_with_slurm_request();
    	
        if($run eq ""){# attend que les jobs soumis se terminent pour rendre la main.
        	print "DONE RUN ".$self -> getNumberRun()."\n";
        }else{
        	$run .= "FAIL RUN ".$self -> getNumberRun()."\n";
        	die($run);
        }
        ### Reinitialise le tableau des taches à zéro, si deux run dans un même script de pipeline.
        my @tab = ();
        $self->{ TASKS } = \@tab;
    }  
}

=head2 _waiting_with_slurm_request

 Nom       : _waiting_with_slurm_request
 Fonction  : PRIVATE. Suspend l'exécution du wrapper tant que tous les jobs du run ne sont pas terminés.
 Exemple   : $self -> _waiting_with_slurm_request();
 Retour    : Null.
 Arguments : Null.

=cut
sub _waiting_with_slurm_request {
	my $self = shift;
	
	my $errorMessage = "";
	
	while( @{$self -> getTasksWithStatus(0)} > 0 and !$JOB_FAIL){
        sleep(5); # slurm à besoin de quelque temps pour mettre a jours les etats des jobs
		$self -> setJobsInfos(); # Mise à jours des infos sur les jobs soumis et non terminés
    }
    if($JOB_FAIL){
		foreach(@{$self -> getTasksWithStatus(0)}){ # on annule tous les jobs qui ont été soumis et qui ne sont pas terminés.
			my $task = $_;
			
			if($self -> testIfSlurmTask($task)){
				$task -> cancel();
			}
		}
		sleep(5); # slurm à besoin de quelque temps pour mettre a jours les etats des jobs.
    	$self -> setJobsInfos(); # Mise à jours des infos sur les jobs soumis et non terminés.
    	$errorMessage .= "---\n";
    	foreach(@{$self -> getTasksWithStatus(0)}){
			my $nonCompletedTask = $_;
		    # ecriture dans les fichiers history
		    my $history = $_ -> historyFileToString();
        	$_ -> toTaskHistoryFile($history);
        	$self -> toManagerHistoryFile($history);
		    
		    my $pathErr;
		    if($self -> testIfSlurmTask($nonCompletedTask)){
		    	$pathErr = $nonCompletedTask -> getOutDir()."/".$nonCompletedTask -> getID().".err";
		    }else{
		    	$pathErr = $nonCompletedTask -> getOutDir()."/".$nonCompletedTask -> getName().".err";
		    }
		    
    		$errorMessage .= "Job named ".$nonCompletedTask -> getName()." (".$nonCompletedTask -> getID().") ".$nonCompletedTask -> getState()." (".$nonCompletedTask -> getReturn().")\n";
    		
    		if (-e $pathErr and !-z $pathErr){ # si le fichier d'erreur existe et n'est pas vide, afficher les erreurs.
    			$errorMessage .= "Message d'erreur : ";
    			if(open(FILE,"<".$pathErr)){
					my @errMessage = <FILE>;
		     	  	for(@errMessage){
		            	$errorMessage .= $_;
		        	}
		        	close(FILE);
		     	}
    		}
    		$errorMessage .= "---\n";
    	}
    }
    my $infosOnWorkflow = $self -> infosOnWorkflow();
    print $infosOnWorkflow;
    print {$self -> getOutHandle()} "Infos on run ".$self -> getNumberRun()."\n".$infosOnWorkflow;
    
    my %workflowInfos = %{$self->getWorkflowInfos()};
    $self -> toManagerHistoryFile("\t<RUN_INFOS number=\'".$self -> getNumberRun()."\' numberOfJobs=\'".$workflowInfos{"jobsNbr"}."\' durationOfJobs=\'".$workflowInfos{"sumJobDuration"}."\' ressourceUsed=\'".$workflowInfos{"sumJobRessource"}."\'></RUN_INFOS>\n</RUN>\n");
	
	return $errorMessage;
}

=head2 setJobsInfos

 Nom       : setJobsInfos
 Fonction  : Retourne les taches slurm soumis par l'utilisateur depuis le debut du manager.
 Exemple   : $runner -> setJobsInfos();
 Retour    : boolean, 1 si ok, 0 sinon
 Arguments : REF, sur un tableau d'objet Tasks.

=cut
sub setJobsInfos{
	my $self = shift;
	
	my @tabJobs = @{$self -> getTasksWithStatus(0)}; # recupération des jobs soumis et non terminés.
	
	if(int(@tabJobs) > 0){
		my @tabJobsIds = ();
		my %hashSacctInfos;
		
		foreach(@tabJobs){
			push(@tabJobsIds, $_ -> getID());
		}
		my $listeJobId = join(",", @tabJobsIds);	    
		# on construit une table de hash avec les ids des jobs comme clé une autre hash comme valeur (clé champ et valeur valeur).
		my $checkStateCmd = "sacct -j ".$listeJobId." --format=jobId,state,submit,start,end,exitcode -X --noheader";
		#print $checkStateCmd."\n";
		my @sacctOut = `$checkStateCmd`; # récupération des statuts des jobs soumis depuis la création du Manager.
	    foreach(@sacctOut){ 
			my $lineJobState = $_;
			my @taskInfos = split(" ", $lineJobState);
			$hashSacctInfos{$taskInfos[0]}{"state"} = $taskInfos[1];
			$hashSacctInfos{$taskInfos[0]}{"submit"} = $taskInfos[2];
			$hashSacctInfos{$taskInfos[0]}{"start"} = $taskInfos[3];		
			$hashSacctInfos{$taskInfos[0]}{"end"} = $taskInfos[4];
			$hashSacctInfos{$taskInfos[0]}{"exitcode"} = $taskInfos[5];
	    }
	    foreach(@tabJobs){
	    	my $task = $_;
		    my $taskId = $task -> getID();
		    
		    if(defined $hashSacctInfos{$taskId}{"state"}){
				$task -> setState($hashSacctInfos{$taskId}{"state"}); # on met à jours l'état de la tâche dans l'objet Task.
		    }
			if(defined $hashSacctInfos{$taskId}{"submit"}){
				$task -> setSubmitTime($hashSacctInfos{$taskId}{"submit"});
			}
			if(defined $hashSacctInfos{$taskId}{"start"}){
		    	$task -> setBegin($hashSacctInfos{$taskId}{"start"});
			}
		    if(defined $hashSacctInfos{$taskId}{"end"}){
		    	$task -> setEnd($hashSacctInfos{$taskId}{"end"});
		    }
		    if(defined $hashSacctInfos{$taskId}{"exitcode"}){
		    	$task -> setReturn($hashSacctInfos{$taskId}{"exitcode"});
		    }
		    
		    if($task -> getState() =~ m/COMPLETED/ and $task -> getReturn() eq "0:0"){
        		my $history = $_ -> historyFileToString();# on archive les logs des jobs COMPLETED et sans erreur dans le history de la tache et le allHistory du Manager 
        		$_ -> toTaskHistoryFile($history);
        		$self -> toManagerHistoryFile($history);
        		$_ -> setStatus(1);
		    }elsif($self -> testIfSlurmTask($task) and $task -> taskFailed()){
				$JOB_FAIL = 1;
        	}
	    }
	    return 1;
	}else{
		print "Pas de jobs a mettre a jours ! \n";
		return 0;
	}
}

=head2 getSlurmTasks

 Nom       : getSlurmTasks
 Fonction  : Retourne uniquement les taches slurm parmis toutes les taches du manager.
 Exemple   : my @slurmTasks = @{$self -> getSlurmTasks()};
 Retour    : REF; Réference sur un tableau de Tasks Slurm.
 Arguments : Null.

=cut
sub getSlurmTasks{
	my $self = shift;
	my @slurmTasks = ();
	
	foreach(@{$self -> getTasks()}){
		my $task = $_;
		if($self -> testIfSlurmTask($task)){
			push(@slurmTasks, $task);
		}
	}
	return \@slurmTasks;
}

=head2 testIfSlurmTask

 Nom       : testIfSlurmTask
 Fonction  : Test s'il s'agit d'une tache Slurm.
 Exemple   : $self -> testIfSlurmTask($task);
 Retour    : boolean, 1 si tache slurm, 0 sinon.
 Arguments : OBJECT : Task.

=cut

sub testIfSlurmTask(){
	my ($self, $task) = @_;
	
	if(ref($task) eq "TaskManager2::TaskSlurmCns" || ref($task) eq "TaskManager2::TaskSlurmCng" || ref($task) eq "TaskManager2::TaskSlurmCcrt" || ref($task) eq "TaskManager2::TaskSlurmCnsGlost" ||  ref($task) eq "TaskManager2::TaskSlurmCngGlost" ||  ref($task) eq "TaskManager2::TaskSlurmCcrtGlost"){
		return 1;
	}else{
		return 0;
	}
}

=head2 _checkLimitSubmitJob

 Nom       : _checkLimitSubmitJob
 Fonction  : PRIVATE. Vérifie le nombre max de job soumis n'est pas atteinte. Max 32 jobs en QOS long.
 Exemple   : my $boolean = $self -> _checkLimitSubmitJob();
 Retour    : boolean, 1 si limite n'est pas atteinte, 0 sinon.
 Arguments : Null.

=cut
sub _checkLimitSubmitJob{
    my $self = shift;
    
    my $cmdToSubmit = "";
    my $numberOfSubmitedJob = 0;
    
    $cmdToSubmit = "squeue -o \%i -u \$USER";
    my @OUT = `$cmdToSubmit`;
    $numberOfSubmitedJob = scalar(@OUT) - 1;
    
    # check la limite de la QOS long
    $cmdToSubmit = "squeue -o \%i -u \$USER -q long";
    @OUT = `$cmdToSubmit`;
    my $numberOfSubmitedJobQosLong = scalar(@OUT) - 1;
    
    if($numberOfSubmitedJob < $self -> getMaxSubmitJobPerUser() && $numberOfSubmitedJobQosLong < 30){
        return 1;
    }else{
    	if(!($numberOfSubmitedJobQosLong < 30)){
    		print "La limite de 30 jobs soumis par l'UTILISATEUR avec la QOS long est depassee, attente que les jobs deja soumis se terminent pour soumettre les jobs suivants...\n";
    	}
        return 0;
    }
}

=head2 _checkLimitSubmitJobForWorkflow

 Nom       : _checkLimitSubmitJobForWorkflow
 Fonction  : PRIVATE. Vérifie le nombre max de job en parallele pour le workflow n'est pas atteinte.
 Exemple   : my $boolean = $self -> _checkLimitSubmitJobForWorkflow();
 Retour    : boolean, 1 si limite n'est pas atteinte, 0 sinon.
 Arguments : Null.

=cut
sub _checkLimitSubmitJobForWorkflow{
    my $self = shift; 
    
    if(int(@{$self -> getTasksWithStatus(0)}) < $self -> getMaxProc()){
    	return 1;
    }else{
    	sleep 5;
    	$self -> setJobsInfos();  # on met à jours les infos sur les jobs soumis et non terminés.
    	if(int(@{$self -> getTasksWithStatus(0)}) < $self -> getMaxProc()){
    		return 1;
    	}else{
    		return 0;
    	}
    }
}


sub cancelAllSubmitedTask(){
    my $self = shift;
    
    my $str = "";
    my $graph = $self -> _getGraph();
    my @taskNoSubmit = $graph -> vertices();# recupere toutes les taches non soumises
    my @allTask = @{$self -> getTasks()};
    
    sub _array_minus2(\@\@) {# revoie les objets presents dans le premier tableau et absents du deuxieme.
       	my %e = map{ $_ => undef } @{$_[1]};
       	return grep( ! exists( $e{$_} ), @{$_[0]} ); 
    }
    
    my @taskSubmit = _array_minus2(@allTask, @taskNoSubmit);# retourne les taches qui ont été soumises avec succèe
    $str .= "\n*********************************************************************************\n" if $DEBUG;
    $str .= "[".ref($self)."] All tasks are canceled..." if $DEBUG;
    $str .= "\n*********************************************************************************\n" if $DEBUG;
    foreach(@taskSubmit){
        if(ref($_) eq "TaskManager2::TaskSlurmCns" || ref($_) eq "TaskManager2::TaskSlurmCnsGlost" || ref($_) eq "TaskManager2::TaskSlurmCcrt" || ref($_) eq "TaskManager2::TaskSlurmCcrtGlost"){
             my $task = $_;
             $task -> cancel();
        }
    }
    return $str;
}

=head2 _toGlostTask

 Nom       : _toGlostTask
 Fonction  : PRIVATE. Rassemble des tâches paralleles dans GLOST.
 Exemple   : $self -> _toGlostTask($graph, $limiteToGlost);
 Retour    : REF GRAPH.
 Arguments : REF GRAPH, NUM le nombre de taches a partir du quel faire une tache Glost.

=cut
sub _toGlostTask(){
    my ($self, $graph, $limiteToGlost) = @_;
    my @tasksTab = ();
    
    sub _array_minus(\@\@) {# revoie les objets presents dans le premier tableau et absents du deuxieme.
      	my %e = map{ $_ => undef } @{$_[1]};
      	return grep( ! exists( $e{$_} ), @{$_[0]} ); 
    }

    sub _exist{# dans la list, exist il l'objet (_exist(list,objet))
        my ($tabRef, $object) = @_;
        foreach(@{$tabRef}){
            if(refaddr($_) == refaddr($object)){
                return 1;
            }
        }
        return 0;
    }

    while($graph -> vertices()){ # tant qu'il y des noeuds dans le graph.
        my @sinkVertices = $graph -> sink_vertices();# récupere toutes les feuilles du graph (tache sans dep mais dont dépendent d'autres taches)
        my @isolatedVertices = $graph -> isolated_vertices();# récupere les noeuds isolés du graph, sans père ni fils (tache sans dep et dont aucun autre taches depend)         
        my @tasksToCheck = (@sinkVertices, @isolatedVertices);
        
        if(@tasksToCheck == 0){
            die ("[".ref($self)."] Boucle dans la graphe de dependance !\n");
        }
        my @predecessors = ();
        my @successors = ();
        my %modules = ();
        
        if(scalar(@tasksToCheck) >= $limiteToGlost){
             my @cmds = ();
             my $task;
             foreach(@tasksToCheck){
                 $task = $_;
                 push(@cmds, $task -> getCmd());
               
                 foreach(@{$task -> getDep()}){
                    if(_exist(\@successors, $_) == 0){
                        push(@successors, $_);
                    }
                 }
                 
                 foreach($graph -> predecessors($task)){
                     if(_exist(\@predecessors, $_) == 0){
                         push(@predecessors, $_);
                     }
                 }
                 
                 foreach(@{$task -> getModules()}){
                 	$modules{$_}++;
                 }
             }
             
             my $ressourceForGlostTask = 0;
             if($self -> getLimitToGlost() >= $self -> getMaxRessource()){
                 $ressourceForGlostTask = $self -> getMaxRessource();
             }else{
                 $ressourceForGlostTask = (scalar(@cmds)+1)*$task -> getNbrCorePerTask();
                 while($ressourceForGlostTask > $self -> getMaxRessource()){
                     $ressourceForGlostTask = $ressourceForGlostTask - $task -> getNbrCorePerTask();
                 }
             }
             # 
             my %slurmJobOptsToSubmit = %{TaskManager2::ManagerFactory -> _setSlurmOpts($task -> getOptsTask(), "-n ". int($ressourceForGlostTask/$task -> getNbrCorePerTask())." -c ".$task -> getNbrCorePerTask())};
             
             
		     my $newOptsForSlurmGlostTask = "";
		    
		     foreach my $key (keys(%slurmJobOptsToSubmit)){
		    	if(defined $slurmJobOptsToSubmit{$key}){
		    	}
		        if(defined $slurmJobOptsToSubmit{$key} && ref($slurmJobOptsToSubmit{$key}) ne "ARRAY"){
		            my $opt = "-".$key." ".$slurmJobOptsToSubmit{$key}." ";
		            $newOptsForSlurmGlostTask .= $opt;
		        }
		     }
             
             my $glostTask = $self -> createJob("G.".$task -> getName(), \@cmds, undef, "glost", $newOptsForSlurmGlostTask);
             
             $glostTask -> addDep(\@successors);
             
             foreach(keys(%modules)){
             	$glostTask -> addModule($_);
             }
             foreach(@predecessors){
                 my @taskDep = @{$_ -> getDep()};
                 my @newDep = _array_minus(@taskDep, @tasksToCheck);
                 push(@newDep, $glostTask);
                 $_ -> setDep(\@newDep);
             }
             foreach(@tasksToCheck){
                 $graph -> delete_vertex($_);
             }
             push(@tasksTab, $glostTask);
         }else{
             foreach(@tasksToCheck){
                 my $task = $_;
                 push(@tasksTab, $task);
                 $graph -> delete_vertex($task);
             }
         }
    }
    $self -> setTasks(\@tasksTab);
    return $self -> _setGraph(\@tasksTab);
}

1;