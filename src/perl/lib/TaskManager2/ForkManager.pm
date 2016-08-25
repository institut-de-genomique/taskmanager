################################################################################
#
# * $Id: ForkManager.pm,v 0.1 2014/04/2 akourlai Exp $
#
# * ForkManager module for TaskManager2::ForkManager.pm
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

TaskManager2::ForkManager.pm - Extension Perl utilisant la librairie ForkManager pour executer des taches liées par des contraintes sans batch manager en mode interactif.

=head1 SYNOPSIS

  Voir TaskManager2::ManagerFactory.

=head1 DESCRIPTION

TaskManager2::ForkManager - Extension Perl utilisant la librairie ForkManager pour executer des taches liées par des contraintes sans batch manager en mode interactif.

=head1 AUTHOR

N'hésitez pas à me contacter pour tout renseignement, ajout de fonctionalité ou signalement de bugs.

Artem Kourlaiev, akourlai@genoscope.cns.fr

=cut

package TaskManager2::ForkManager;

use TaskManager2::Manager;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
our @ISA = qw(Exporter TaskManager2::Manager);
# Export par defaut
@EXPORT = qw();
# Export autorisé
@EXPORT_OK = qw();

$VERSION = '0.1';

use strict;
use TaskManager2::ProcessManager;
use Net::Domain qw(hostdomain hostname);
require Exporter;
use POSIX qw(getpid);

our $WORKFLOW_NAME = $TaskManager2::ManagerFactory::WORKFLOW_NAME;

my $DEBUG = 0;

=head2 new

 Nom       : new
 Fonction  : Crée un objet ForkManager.
 Exemple   : $runner = TaskManager2::ForkManager -> new("Nom du wrapper", "./logs");
 Retour    : une référence sur un objet de la classe TaskManager2::ForkManager.
 Arguments : STRING,STRING : Le nom du wrapper et le path pour ces logs.

=cut
sub new {
    my $class = shift;
    my $self = $class -> SUPER::new(@_);
    bless( $self, $class);#lie la référence à la classe courante
    $self -> {STATUS} = 0;
    return $self;
}

=head2 run

 Nom       : run
 Fonction  : Lance l'execution des taches en tenant compte des dependances entre taches.
 Exemple   : $manager -> run();
 Retour    : 0 si tout s'est bien passé.
 Arguments : aucun.

=cut
sub run {
    my $self = shift;
    
    if($self -> getNumberRun() == 0){
    	$self -> _make();
    }

    my $pm = TaskManager2::ProcessManager -> new($self -> getMaxProc());
    my $graph = $self -> _setGraph($self -> getTasks());
       
    $self -> setNumberRun($self -> getNumberRun() + 1);# on incrémente le nombre de fois qu'un run du même Manager a été appelé.
    print "\n>>>>>>>>>>>>>>>>>\nSTART RUN    : ".$self -> getNumberRun()." - $WORKFLOW_NAME\n>>>>>>>>>>>>>>>>>\n" if $DEBUG;
    $self -> toManagerHistoryFile("<RUN number=\'".$self -> getNumberRun()."\' maxProcessInParallel=\'".$self -> getMaxProc()."\'>\n");
    ######### Run on start ############
    $pm->run_on_start(
        sub { 
        	my ($pid, $ident) = @_;
        	
        	my $task = $self -> getTask($ident);
        	if (!defined $task) {
        	    die "[".ref($self)."] La tache portant le nom $ident est introuvable.\n";
        	}
        	$task -> setBegin();
        	$task -> setID($pid);
        	$task -> setStatus(0);
        	$task -> setStatus(0);
            print "GO  ".$task -> getName()." ".localtime(time)." Host domaine : ".hostname()."\n";
         });   
   
    ######### Run on finish ###########
    $pm -> run_on_finish(
        sub {
        	my ($pid, $exit_code, $ident) = @_;
        	my $task = $self -> getTask($ident);
        	if (!defined $task) {
        	    die "[".ref($self)."] La tache portant le nom $ident est introuvable.\n";
        	}
        	$task -> setEnd();
        	$task -> setReturn($exit_code);
        	$task -> setStatus(1);
        	
        	print "END ".$task -> getName()." ".localtime(time)." Host domaine : ".hostname()."\n";
        	my $history = $task -> historyFileToString();
        	$task -> toTaskHistoryFile($history);
        	$self -> toManagerHistoryFile($history);
            
            if ($exit_code){
                my $str;
                $str .= "---\n";
                $str .= "Task named ".$task -> getName()." (".$task -> getID().") fail with ".$task -> getReturn()." error\n";
                
                
                $pm -> wait_all_children;
                my $pathErr = glob ($task -> getOutDir()."/*.err");
               
                if(open(FILE,"<".$pathErr)){
                    my @errMessage = <FILE>;
                    for(@errMessage){
                       $str .= $_;
                   }
                   close(FILE);
                }
                print $self -> infosOnWorkflow();
                $str .= "FAIL RUN ".$self -> getNumberRun()."\n";
                
        	    die $str;
        	}        	
        });
   
    while($graph -> vertices()){ #tant qu'il y des noeuds dans le graph.
        my @sinkVertices = $graph -> sink_vertices(); # récupere toutes les feuilles du graph (tache sans dep mais dont dépendent d'autres taches)
        my @isolatedVertices = $graph -> isolated_vertices();# récupere les noeuds isolés du graph, sans pére ni fils (tache sans dep et dont aucun autre taches depend)         
        my @tasksToSubmit = (@sinkVertices, @isolatedVertices);
            
        if(@tasksToSubmit == 0){
           die "[".ref($self)."] Boucle dans le graphe de dependance !\n";
        }    
		
        while(scalar(@tasksToSubmit) != 0){
            my $task = pop @tasksToSubmit;

            if($task -> getStatus() == -1){ # Si l'exécution de la tache n'a pas encore été lancé.
                $task -> setOutDir($self -> getOutDir());# affecte le outDir du Manager au outDir de la tache.
                $task -> make();# crée le répertoire de la tache et y enregistre le run.sh contenant le commande.
          	    my $pid = $pm -> start($task -> getName());
          	    $task -> makeCmdToExecute();
          	    if (!$pid) {
              		# This code is the child process
              		$task -> setID(POSIX::getpid());
              		
              		print {$self -> getOutHandle()} $self -> toStringTaskInfos($task);
              		
              		my $excCmd = $task -> getCmdToExecute();
              		my $ret_sys = exec($excCmd);
              		my $ret = $? >> 8; # valeur de sortie (code erreur) 
              		
              		#my $graph = $self -> _getGraph();
              		$pm -> finish($ret);
              		
          	    }
            }
            if($task -> getStatus() == 1){ # Si la tache est terminé, on la supprime du graphe.
                $graph -> delete_vertex($task); #supprime la tache du graph
            }
            sleep(1);
        }
    	$pm -> wait_a_child();
    	#print "##### get verticves ##########\n";
    }
    $pm -> wait_all_children;
    my $infosOnWorkflow = $self -> infosOnWorkflow();
    print $infosOnWorkflow;
    print {$self -> getOutHandle()} "Infos on run ".$self -> getNumberRun()."\n".$infosOnWorkflow;
    print "DONE RUN ".$self -> getNumberRun()."\n";
    
    my %workflowInfos = %{$self->getWorkflowInfos()};
    $self -> toManagerHistoryFile("\t<RUN_INFOS number=\'".$self -> getNumberRun()."\' numberOfTasks=\'".$workflowInfos{"jobsNbr"}."\' durationOfTasks=\'".$workflowInfos{"sumJobDuration"}."\' ressourceUsed=\'".$workflowInfos{"sumJobRessource"}."\'>\n</RUN>\n");

	print "<<<<<<<<<<<<<<<<<\nEND RUN      : ".$self -> getNumberRun()." - $WORKFLOW_NAME\n<<<<<<<<<<<<<<<<<\n" if $DEBUG;
	$self -> {TASKS} = undef;
}
		
1;