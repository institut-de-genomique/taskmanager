################################################################################
#
# * $Id: Manager.pm,v 0.1 2014/04/2 akourlai Exp $
#
# * Manager module for TaskManager2::Manager.pm
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

TaskManager2::Manager.pm - Class abstraite. Permet d'ordonner les soumissions/exécution de taches suivant leurs dépendances.

=head1 SYNOPSIS

    Classe Abstraite. Utiliser TaskManager2::ManagerFactory pour instancier un Manager.

=head1 DESCRIPTION

TaskManager2::BatchManager::Manager Class abstraite. Permet d'ordonner les soumissions/exécution de taches suivant leurs dépendances.

=head1 AUTHOR

N'hésitez pas à me contacter pour tout renseignement, ajout de fonctionalité ou signalement de bugs.

Artem Kourlaiev, akourlai@genoscope.cns.fr

=cut

package TaskManager2::Manager;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
@ISA = qw(Exporter);
# Export par defaut
@EXPORT = qw();
# Export autorisé
@EXPORT_OK = qw();

$VERSION = '0.1';

use strict;
use File::Temp qw(tempdir tempfile);
use File::Path;
use File::Basename;
use Graph;
use Scalar::Util 'refaddr';
use Time::Local qw(timelocal);
use Carp;

if(!defined ($TaskManager2::ManagerFactory::BM_CMD) or !defined ($TaskManager2::TaskFactory::BM_CMD)){
	croak "[TaskManager2::Manager] Utilisation de la ManagerFactory obligatoire pour instancier TaskManager\n[TaskManager2::Manager] Utilisez la librairie TaskManager2::ManagerFactory \n";
}

use TaskManager2::TaskFactory qw(createTask);
use TaskManager2::ManagerFactory qw(_toStringWorkflowInfos);

require Exporter;

=head2 new

 Nom       : new
 Fonction  : Crée un objet de la classe TaskManager2::Manager.
 Exemple   : my $self = $class -> SUPER::new($name, $outDir);
 Retour    : une référence sur un objet de la classe TaskManager2::Manager.
 Arguments : 2 STRING, le nom du pipepline et le path où créer le dossier out, non obligatoire.

=cut
sub new {
    my $class = shift;
    $class = ref($class) || $class;
    my $self = {};
    bless($self, $class);
    my ($name, $outDir, $maxProcess, $verbose) = @_;
    $self -> setName($name || "Workflow");
    $self -> setOutDir($outDir || "./");
    $self -> setMaxProc($maxProcess || 1);
    $self -> setVerbose($verbose || 0);
    $self -> setNumberRun(0);
    $self -> setManagerStartTime();
    #$self -> _make(); # Crée le répertoire du pipeline est les fichiers de logs. 
    
    return $self;
}

=head2 createJob

 Nom       : createJob
 Fonction  : Crée le job.
 Exemple   : $runner -> createJob("NomDelaTache", "sleep 10");
 Retour    : (OBJECT), l'object Task (une des ces classe fille).
 Arguments : Nom de la tache, la commande, une référence vers un tableau de nom de tache ou le nom d'une tache dont depend la tache à créer. Les arguments suivant ne sont pas gèrés.

=cut
sub createJob {
	my ($self, $jobName, $cmd, $dep, $batch, $opts) = @_;
	
	
	my $taskFactory = TaskManager2::TaskFactory -> new();
	my $job;
	
	$job = $taskFactory -> createTask($jobName, $cmd, $batch || "", $opts, $self -> getOutDir()) or croak; # le outdir va être surchargé dans la methode run de Batch et Fork Manager
	
	$job -> setSubmitTime();
	
    if(defined $dep){
        if(ref($dep) eq 'ARRAY'){
            foreach(@{$dep}) {
                my $taskName = $_;
                my $task;
                if($self -> existTask($taskName)){
                    $task = $self -> getTask($taskName);
                    $job -> addDep($task);
                }else{
                    croak "[".ref($task)."] Impossible de creer une dependance entre ".$taskName." et ".$jobName." (".$taskName." n'existe pas).\n";
                }  
            }
        }else{
             my $taskName = $dep;
             my $task;
             if($self -> existTask($taskName)){
                 $task = $self -> getTask($taskName);
                 $job -> addDep($task);
             }else{
                 croak "[".ref($self)."] Mauvaise dependance specifie a la tache \"".$jobName."\". La tache \"".$taskName."\" n'existe pas.\n";
             }
        }
    }
    #if($self -> existTask($job -> getName()) && ref($self) eq "TaskManager2::ForkManager"){ # Le ForkManager du CPAN ne permet pas de gèré les taches avec le même nom.
    if($self -> existTask($job -> getName())){
    	my $errorMessage = "";
    	$errorMessage .= "[".ref($self)."] Il existe plusieurs taches portant le meme nom !\n";
        $errorMessage .= "[".ref($self)."] Nom en conflit : ".$job -> getName()."\n";
        $errorMessage .= "[".ref($self)."] ".ref($self)." ne permet pas de gerer les taches portant les memes noms !\n";
		croak ($errorMessage);
    }
    $self -> addTask($job);
    return $job;
}

################################################################################
## SETEURS
################################################################################ 
=head2 setMaxSubmitJobPerUser

 Nom       : setMaxSubmitJobPerUser
 Fonction  : Definit le nombre maximal de jobs qu'un utilisateur peut soumettre.
 Exemple   : $runner -> setMaxSubmitJobPerUser(100);
 Retour    : NUMBER, l'ancien nombre de jobs.
 Arguments : NUMBER, le nombre max de jobs soumettables par un utilisateur.

=cut
sub setMaxSubmitJobPerUser {
    my ($self, $arg) = @_;
    if (! defined $arg || ($arg =~ m/\D+/)){
 		croak "[".ref($self)."] La methode \$runner -> setMaxSubmitJobPerUser() prend obligatoirement un argument sous la forme d'un nombre entier.\n"; 
    }
    my $tmp = $self -> getMaxSubmitJobPerUser();
    $self -> { MAX_SUBMIT_JOB_PER_USER } = $arg;
    return $tmp;
}

=head2 setMaxProc

 Nom       : setMaxProc
 Fonction  : Definit le nombre maximal de processus en parallel.
 Exemple   : $runner -> setMaxProc(100);
 Retour    : NUMBER, l'ancien nombre de processus.
 Arguments : NUMBER, le nombre de processus à résérver.

=cut
sub setMaxProc {
    my ($self, $arg) = @_;
    if (! defined $arg || ($arg =~ m/\D+/)){
 		croak "[".ref($self)."] La methode \$runner -> setMaxProc() prend obligatoirement un argument sous la forme d'un nombre entier.\n"; 
    }
    my $tmp = $self -> getMaxProc();
    $self -> { MAX_PROCESS_IN_PARALLEL_PER_WORKFLOW } = $arg;
    return $tmp;
}

=head2 setMaxRessource

 Nom       : setMaxRessource
 Fonction  : Definit le nombre maximal de CPU mobilisable par une tâche.
 Exemple   : $runner -> setMaxRessource(100);
 Retour    : NUMBER, l'ancien nombre de CPU.
 Arguments : NUMBER, le nombre de CPU utilisables par une tâche.

=cut
sub setMaxRessource {
    my ($self, $arg) = @_;
    if (! defined $arg || ($arg =~ m/\D+/)){
 		croak "[".ref($self)."] La methode \$runner -> setMaxRessource() prend obligatoirement un argument sous la forme d'un nombre entier.\n"; 
    }
    my $tmp = $self -> getMaxRessource();
    $self -> { MAX_RESSOURCE } = $arg;
    return $tmp;
}

=head2 setVerbose

 Nom       : set_verbose
 Fonction  : Modifie le drapeau qui active le mode verbose.
 Exemple   : $runner->setVerbose($bool);
 Retour    : Null.
 Arguments : booleen.

=cut
sub setVerbose {
    my ($self, $arg) = @_;
    if (! defined $arg) { croak "[".ref($self)."] La methode \$runner -> setVerbose prend obligatoirement un argument.\n"; }
    $self->{ VERBOSE } = $arg;
    return $arg;
}

=head2 setOutDir

 Nom       : setOutDir
 Fonction  : Permet de définir le path du dossier OUT du wrapper. Dans ce dossier serrons stockés tous les scripts run.sh mais aussi les sorties STDOUT et STDERR de TaskManager.
 Exemple   : $runner -> setOutDir("path/du/dossier/du/wrapper");
 Retour    : STRING, l'ancien path.
 Arguments : STRING, le path du dossier des logs de TaskManager.

=cut
sub setOutDir {
    my ($self, $path) = @_;
    if (! defined $path){
 		croak "[".ref($self)."] La methode \$runner -> setOutDir() prend obligatoirement un argument.\n"; 
    }elsif(! -d $path){
        croak "[".ref($self)."] Mauvais path pour les logs du manager : ".$path."\n";
    }
    my $tmp = $self -> getOutDir();
    $self->{ OUT_DIR } = $path;
    return $tmp;
}

=head2 setOutHandle

 Nom       : setOutHandle
 Fonction  : Definit le handle pour les logs du TaskManager.
 Exemple   : $runner -> setOutHandle($handle);
 Retour    : STRING, l'ancien handle.
 Arguments : STRING, le handle pour les logs de TaskManager.

=cut
sub setOutHandle {
    my ($self, $arg) = @_;
    if (! defined $arg){
 		croak "[".ref($self)."] La methode \$runner -> setOutHandle() prend obligatoirement un argument.\n"; 
    }
    my $tmp = $self -> getOutHandle();
    $self->{ HANDLE_OUT } = $arg;
    return $tmp;
}

=head2 setName

 Nom       : setName
 Fonction  : Permet de définir le nom du wrapper.
 Exemple   : $runner -> setName("ILMAP");
 Retour    : STRING, l'ancien nom.
 Arguments : STRING, le nom du wrapper.

=cut
sub setName {
    my ($self, $arg) = @_;
    if (! defined $arg){
 		croak "[".ref($self)."] La methode \$runner -> setName() prend obligatoirement un argument.\n"; 
    }
    my @a = split(" ", $arg);
    $arg = join("_", @a);
    my $tmp = getName();
    $self -> { NAME } = $arg;
    return $tmp;
}

=head2 _setGraph

 Nom       : _setGraph
 Fonction  : PRIVATE. Crée un graphe orienté à partir des taches et de leurs dépendances en utilisant le module Graph de Perl. A partir du tableau de taches de TaskManager. Chaque noeud est un objet Task et chaque arrête une dépendance. 
 Exemple   : $self -> _setGraph();
 Retour    : (GRAPH) le graphe.
 Arguments : Ref tableau de taches.

=cut
sub _setGraph {
    my ($self, $tabTasks) = @_;
    my @tabTasks = @{$tabTasks};
    
    
    
    my %opt = ("refvertexed" => 1); # permet aux noeuds d'être des objets. En non des chaines de caractères.
    my $graph = Graph->new(%opt);
    
    foreach(@tabTasks){
        my $task = $_;
        $graph -> add_vertex($task);
        
        foreach(@{$task -> getDep()}){
            my $taskDep = $_;
            $graph -> add_edge($task, $taskDep);
        }
    }
    $self -> { GRAPH } = $graph;    
    return $graph;
}

=head2 setNumberRun

 Nom       : setNumberRun
 Fonction  : Compte le nombre de run.
 Exemple   : $runner->setNumberRun(5);
 Retour    : Null.
 Arguments : NUMBER

=cut
sub setNumberRun {
    my ($self, $arg) = @_;
    if (! defined $arg) { croak "[".ref($self)."] La methode \$runner -> setNumberRun() prend obligatoirement un argument.\n"; }
    $self->{ NBR_RUN } = $arg;
    return $arg;
}

=head2 setHistoryFileHandle

 Nom       : setHistoryHandle
 Fonction  : Definit le handle pour le fichier allHistory.xml du TaskManager.
 Exemple   : $runner -> setHistoryFileHandle($handle);
 Retour    : REF, l'ancien handle.
 Arguments : REF, le handle pour le fichier allHistory.xml de TaskManager.

=cut
sub setHistoryFileHandle {
    my ($self, $arg) = @_;
    if (! defined $arg){
 		croak "[".ref($self)."] La methode \$runner -> setHistoryFileHandle() prend obligatoirement un argument.\n"; 
    }
    my $tmp = $self -> getHistoryFileHandle();
    $self->{ HANDLE_HISTORY } = $arg;
    return $tmp;
}

=head2 setHistoryFilePath

 Nom       : setHistoryFilePath
 Fonction  : Definit le path pour le fichier allHistory.xml du TaskManager.
 Exemple   : $runner -> setHistoryFilePath($path);
 Retour    : REF, l'ancien path.
 Arguments : REF, le path du le fichier allHistory.xml de TaskManager.

=cut
sub setHistoryFilePath {
    my ($self, $arg) = @_;
    if (! defined $arg){
 		croak "[".ref($self)."] La methode \$runner -> setHistoryFilePath() prend obligatoirement un argument.\n"; 
    }
    my $tmp = $self -> getHistoryFilePath();
    $self->{ PATH_HISTORY } = $arg;
    return $tmp;
}

=head2 setTasks

 Nom       : setTasks
 Fonction  : Réinitailise le tableau des tâches.
 Exemple   : $runner -> setTasks(\@taskTab);
 Retour    : REF, l'ancien tableau de tâches.
 Arguments : REF, le nouveau tableau de tâches.

=cut
sub setTasks {
    my ($self, $arg) = @_;
    if (! defined $arg){
 		croak "[".ref($self)."] La methode \$runner -> setTasks() prend obligatoirement un argument.\n"; 
    }
    my $tmp = $self -> getTasks();
    $self->{ TASKS } = $arg;
    return $tmp;
}

=head2 setManagerStartTime

 Nom       : setManagerStartTime
 Fonction  : Memorise la date de creation courante ou passe en argument du Manager.
 Exemple   : $runner -> setManagerStartTime("2014-06-30T10:18:06");
 Retour    : L'ancienne date.
 Arguments : Null, ou une date.

=cut
sub setManagerStartTime{
    my ($self, $date) = @_;
	
	my $oldTime = $self -> getManagerStartTime();
    if(!defined $date){
    	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
        my $time =  sprintf("%04d-%02d-%02dT%02d:%02d:%02d",1900+$year,$mon+1,$mday,$hour,$min,$sec);
        $self->{ MANAGER_START_TIME } = $time;
    }else{
        $self->{ MANAGER_START_TIME } = $date;
    }
    return $oldTime;
}

=head2 setManagerEndTime

 Nom       : setManagerEndTime
 Fonction  : Memorise la date de fin courante ou passe en argument du Manager.
 Exemple   : $runner -> setManagerEndTime("2014-06-30T10:18:06");
 Retour    : L'ancienne date.
 Arguments : Null, ou une date.

=cut
sub setManagerEndTime{
    my ($self, $date) = @_;
	
	my $oldTime = $self -> getManagerEndTime();
    if(!defined $date){
    	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
        my $time =  sprintf("%04d-%02d-%02dT%02d:%02d:%02d",1900+$year,$mon+1,$mday,$hour,$min,$sec);
        $self->{ MANAGER_END_TIME } = $time;
    }else{
        $self->{ MANAGER_END_TIME } = $date;
    }
    return $oldTime;
}

=head2 setRandomOutDir

 Nom       : setRandomOutDir
 Fonction  : Initialise la creation d'un repertoire de logs de TaskManager aleatoire ou non.
 Exemple   : $runner -> setRandomOutDir(0);
 Retour    : boolean, l'ancien valeur.
 Arguments : boolean, 1 creation d'un repertoire aleatoire, 0 sinon.

=cut
sub setRandomOutDir{
    my ($self, $boolean) = @_;
	
	my $old = $self -> getRandomOutDir();
	$self->{ MANAGER_OUTDIR_TYPE } = $boolean;
    return $old;
}

################################################################################
## GETEURS
################################################################################

=head2 getMaxSubmitJobPerUser

 Nom       : getMaxSubmitJobPerUser
 Fonction  : Retourne le nombre maximum de jobs executables en parallel par le meme TaskManager.
 Exemple   : $runner -> getMaxSubmitJobPerUser();
 Retour    : NUMBER, le nombre de processus alloués au TaskManager.
 Arguments : Null.

=cut
sub getMaxSubmitJobPerUser {
    my $self = shift;
    if(! defined $self -> {MAX_SUBMIT_JOB_PER_USER}){
        $self -> {MAX_SUBMIT_JOB_PER_USER} = 120;
    }
    return $self -> { MAX_SUBMIT_JOB_PER_USER };
}

=head2 getMaxProc

 Nom       : getMaxProc
 Fonction  : Retourne le nombre de processus maximum en parallel lancés avec par TaskManager.
 Exemple   : $runner -> getMaxProc();
 Retour    : NUMBER, le nombre de processus alloués au TaskManager.
 Arguments : Null.

=cut

sub getMaxProc {
    my $self = shift;
    if(! defined $self -> {MAX_PROCESS_IN_PARALLEL_PER_WORKFLOW}){
        $self -> {MAX_PROCESS_IN_PARALLEL_PER_WORKFLOW} = 1;
    }
    return $self -> { MAX_PROCESS_IN_PARALLEL_PER_WORKFLOW };
}

=head2 getMaxRessource

 Nom       : getMaxRessource
 Fonction  : Retourne le nombre maximal de CPU utilisables par une tâche.
 Exemple   : $runner -> getMaxRessource();
 Retour    : NUMBER, le nombre de CPU utilisables par une tâche.
 Arguments : Null.

=cut
sub getMaxRessource {
    my $self = shift;
    if(! defined $self -> {MAX_RESSOURCE}){
        $self -> {MAX_RESSOURCE} = 1;
    }
    return $self -> { MAX_RESSOURCE };
}

=head2 getVerbose

 Nom       : getVerbose
 Fonction  : Renvoie le drapeau qui active ou non le mode verbose.
 Exemple   : $verbose = $runner -> getVerbose();
 Retour    : booleen.
 Arguments : aucun.

=cut
sub getVerbose {
    my $self = shift;
    return $self->{ VERBOSE };
}

=head2 getOutDir

 Nom       : getOutDir
 Fonction  : Renvoie le path du dossier de logs de TaskManager.
 Exemple   : $runner -> getOutDir();
 Retour    : STRING, le path du dossier de logs de TaskManager.
 Arguments : Null.

=cut
sub getOutDir {
    my $self = shift;    
    if(!defined $self -> { OUT_DIR }) {
        $self -> { OUT_DIR } = "./";
    }
    return $self->{ OUT_DIR };
}

=head2 getOutHandle

 Nom       : getOutHandle
 Fonction  : Renvoie le handle du fichier des logs de TaskManager.
 Exemple   : $runner -> getOutHandle();
 Retour    : REF, le handle du fichier des logs de TaskManager.
 Arguments : Null.

=cut
sub getOutHandle {
    my $self = shift;    
    if(!defined $self -> { HANDLE_OUT }) {
        $self -> { HANDLE_OUT } = "";
    }
    return $self->{ HANDLE_OUT };
}

=head2 getName

 Nom       : getName
 Fonction  : Renvoie le nom du wrapper.
 Exemple   : $runner -> getName();
 Retour    : STRING, l'ancien nom du wrapper.
 Arguments : Null.

=cut
sub getName {
    my $self = shift;
    if(!defined $self -> { NAME }){
        $self -> { NAME } = "";
    }
    return $self -> { NAME };
}

=head2 _getGraph

 Nom       : _getGraph
 Fonction  : Renvoie la réference du graph de dépedances des taches.
 Exemple   : $runner -> _getGraph();
 Retour    : OBJECT GRAPH
 Arguments : Null.

=cut
sub _getGraph {
    my $self = shift;
    if(!defined $self -> { GRAPH }){
        $self -> { GRAPH } = undef;
    }
    return $self -> { GRAPH };
}

=head2 getTasks

 Nom       : getTasks
 Fonction  : Renvoie l'ensemble des taches associées au wrapper.
 Exemple   : $runner -> getTasks();
 Retour    : (Référence), la référence vers le tableau des taches à manager (tableau de références pointants vers les objects Task)
 Arguments : Null

=cut
sub getTasks {
    my $self = shift;
    if(!defined $self->{ TASKS }){
        my @tabTasks = ();
        $self->{ TASKS } = \@tabTasks;
    }
    return $self->{ TASKS };
}

=head2 getTasksWithStatus

 Nom       : getTasksWithStatus
 Fonction  : Renvoie l'ensemble des Tasks avec le status correspondant.
 Exemple   : $runner -> getTasksWithStatus();
 Retour    : (Référence), la référence vers le tableau des Tasks avec le bon status.
 Arguments : INT. -1 (non soumis ni executé), 0 (soumis ou en execution), 1 (fini).

=cut
sub getTasksWithStatus {
    my ($self, $status) = @_;
    
    my @taskWithGoodStatus = ();
    
    if(!defined $status){
        croak "[".ref($self)."] La methode \$runner -> getTasksWithStatus() prend obligatoirement un argument.\n"; 
    }else{
    	foreach(@{$self -> getTasks()}){
    		if($_ -> getStatus() == $status){
    			push(@taskWithGoodStatus, $_);
    		}
    	}
    }
    return \@taskWithGoodStatus;
}

=head2 getTaskWithId()

 Nom       : getTaskWithId
 Fonction  : Renvoie la tâche selon son ID.
 Exemple   : $runner -> getTaskWithId("123456");
 Retour    : (Référence), la référence vers l'object Task.
 Arguments : STRING, le ID de la tâche.

=cut
sub getTaskWithId {
    my ($self, $taskId) = @_;
    
    foreach(@{$self -> getTasks()}){
    	if($_ -> getID() eq $taskId){
    		return $_;
    	}
    }
    return undef;
}

=head2 getNumberRun

 Nom       : getNumberRun
 Fonction  : Renvoie le nombre de runs effectué avec le même Manager.
 Exemple   : $numberOfRun = $runner -> getNumberRun();
 Retour    : Number.
 Arguments : aucun.

=cut
sub getNumberRun {
    my $self = shift;
    return $self->{ NBR_RUN };
}

=head2 getHistoryFileHandle

 Nom       : getHistoryFileHandle
 Fonction  : Renvoie le handle du fichier allHistory.xml de TaskManager.
 Exemple   : $runner -> getHistoryFileHandle();
 Retour    : REF, le handle du fichier allHistory.xml de TaskManager.
 Arguments : Null.

=cut
sub getHistoryFileHandle {
    my $self = shift;    
    if(!defined $self -> { HANDLE_HISTORY }) {
        $self -> { HANDLE_HISTORY } = "";
    }
    return $self->{ HANDLE_HISTORY };
}

=head2 getHistoryFilePath

 Nom       : getHistoryFilePath
 Fonction  : Renvoie le path du fichier allHistory.xml de TaskManager.
 Exemple   : $runner -> getHistoryFilePath();
 Retour    : REF, le path du fichier allHistory.xml de TaskManager.
 Arguments : Null.

=cut
sub getHistoryFilePath {
    my $self = shift;    
    if(!defined $self -> { PATH_HISTORY }) {
        $self -> { PATH_HISTORY } = "";
    }
    return $self->{ PATH_HISTORY };
}

=head2 getFailTasks

 Nom       : getFailTasks
 Fonction  : Renvoie la réference du tableau avec les taches en echecs.
 Exemple   : my $errorTasksTab = $runner -> getFailTasks();
 Exemple2  : my $errorTasksTab = $runner -> getFailTasks("nomDeLaTache");
 Retour    : REF, la réference du tableau de taches en erreur.
 Arguments : Null ou STRING.

=cut
sub getFailTasks {
    my ($self, $name) = @_;
	
	my @failTasksTab = ();
	
	if (defined $name){
		my $task = $self -> getTask($name);
		if ($task -> getReturn() ne "0" and $task -> getReturn() ne "") {
	    	push (@failTasksTab, $task);
		}
	}else{
		foreach(@{$self->getTasks()}) {
	    	if ($_ -> getReturn() ne "0" and $_ -> getReturn() ne "") {
	    	    push (@failTasksTab, $_);
	    	}
	    }
	}
    return \@failTasksTab;
}

=head2 getManagerStartTime

 Nom       : getManagerStartTime
 Fonction  : Renvoie le moment de creation du Manager.
 Exemple   : $runner -> getManagerStartTime();
 Retour    : STRING, le moment de soumission (YYYY-MM-DDTHH:MM:SS)
 Arguments : Null.

=cut
sub getManagerStartTime {
    my $self = shift;
    if(!defined $self->{ MANAGER_START_TIME }){
        $self->{ MANAGER_START_TIME } = "Unknown";
    }
    return $self->{ MANAGER_START_TIME };
}

=head2 getManagerEndTime

 Nom       : getManagerEndTime
 Fonction  : Renvoie le moment de fin du Manager.
 Exemple   : $runner -> getManagerEndTime();
 Retour    : STRING, le moment de soumission (YYYY-MM-DDTHH:MM:SS)
 Arguments : Null.

=cut
sub getManagerEndTime {
    my $self = shift;
    if(!defined $self->{ MANAGER_END_TIME }){
        $self->{ MANAGER_END_TIME } = "Unknown";
    }
    return $self->{ MANAGER_END_TIME };
}

=head2 getRandomOutDir

 Nom       : getRandomOutDir
 Fonction  : Renvoie le type du repertoire de log de TaskManager, random ou non.
 Exemple   : $runner -> getRandomOutDir();
 Retour    : boolean. 1 creation d'un repertoire aleatoire. 
 Arguments : Null.

=cut
sub getRandomOutDir {
    my $self = shift;
    if(!defined $self->{ MANAGER_OUTDIR_TYPE }){
        $self->{ MANAGER_OUTDIR_TYPE } = 1;
    }
    return $self->{ MANAGER_OUTDIR_TYPE };
}

################################################################################
## METHODES
################################################################################

=head2 addTask

 Nom       : addTask
 Fonction  : Rajoute une tache au Manager.
 Exemple   : $runner -> addTask($task);
 Retour    : Null
 Arguments : OBJECT, la tache (Task).

=cut
sub addTask {
    my ($self, $task) = @_;
        
    if (! defined $task){
	    croak "[".ref($self)."] La methode \$runner -> addTask() prend obligatoirement un argument.\n";
	}
	push(@{$self -> getTasks()}, $task);
}

#### ATTENTIOn une methode à rajouter dans task ######
=head2 removeDep

 Nom       : removeDep
 Fonction  : Supprime la tache passé en parametre des dépandances des autres taches.
 Exemple   : $runner->removeDep($task);
 Retour    : aucun.
 Arguments : OBJECT TASK, la tache à supprimer des dépendances des autres taches du Manager.

=cut
sub removeDep {
    my ($self, $task) = @_;
    foreach(@{$self->getTasks()}) {
	    $_ -> remove_dep($task->getName());
    }
}

=head2 getTask

 Nom       : getTask
 Fonction  : Renvoie la tache dont le nom est donné en argument.
 Exemple   : my $task = $runner->getTask("tache1");
 Retour    : OBJECT Task, une référence sur un objet de la classe TaskManager2::Task ou undef.
 Arguments : le nom de la tache recherchée.

=cut
sub getTask {
    my ($self, $name) = @_;
    foreach(@{$self->getTasks()}) {
    	if ($_ -> getName() eq $name) {
    	    return $_;
    	}
    }
    return undef;
}

=head2 existTask

 Nom       : existTask
 Fonction  : Renvoie 1 si la tache dont le nom est donné en argument existe, 0 sinon.
 Exemple   : my $exist = $runner->existTask("tache1");
 Retour    : 0 ou 1.
 Arguments : le nom de la tache recherché.

=cut
sub existTask {
    my ($self, $name) = @_;
    my $cmp = 0;
    
    foreach(@{$self -> getTasks()}) {
    	if ($_ -> getName() eq $name) {
    	    $cmp++;
    	}
    }
    return $cmp;
}



=head2 _makeOutDir

 Nom       : _makeOutDir
 Fonction  : PRIVATE. Crée le dossier OUT du TaskManager.
 Exemple   : $self -> _makeOutDir();
 Retour    : L'ancien path du dossier OUT du TaskManager.
 Arguments : Null.

=cut
sub _makeOutDir {
    my $self = shift;
    my $dir;
    
    if($self -> getRandomOutDir()){
    	$dir = tempdir( $self -> getName()."_XXXX", DIR => $self->getOutDir(), TMPDIR => 1, CLEANUP => 0 );
    }else{
    	$dir = $self->getOutDir()."/".$self -> getName();
    	
    	mkdir ($dir) or croak ("[".ref($self)."] Erreur creation repertoire de log de TaskManager ".$dir." : $!\n");
    }
    
    my $tmp = $self -> setOutDir($dir);
   
    system ("chmod 2755 $dir");
    
    return $tmp;
}

=head2 _makeOutFile

 Nom       : _makeOutFile
 Fonction  : PRIVATE. Crée le fichier des logs de TaskManager.
 Exemple   : $self -> _makeOutFile();
 Retour    : BOOLEAN. 1 si creation du fichier.
 Arguments : Null.

=cut
sub _makeOutFile {
    my $self = shift;
    
    my $fileHandle;
    my $path = $self -> getOutDir()."/infos.log";
    
    open($fileHandle,">>".$path) or croak("[".ref($self)."] Problème fichier des logs du Manager: open: $!\n");
    if($self -> getVerbose()){
        my $sortie = select($fileHandle);
    }
    #$self->{ HANDLE_OUT }->autoflush(1);
    $self -> setOutHandle($fileHandle);
    
    system ("chmod 644 $path");
    return 1;
}

=head2 _makeHistoryFile

 Nom       : _makeOutFile
 Fonction  : PRIVATE. Crée le fichier allHistory.xml de TaskManager.
 Exemple   : $self -> _makeHistoryFile();
 Retour    : BOOLEAN. 1 si la création du fichier allHistory.xml de TaskManager.
 Arguments : Null.

=cut
sub _makeHistoryFile {
    my $self = shift;
    
    my $fileHandle;
    my $path = $self -> getOutDir()."/allHistory.xml";
    open($fileHandle,">>".$path) or croak("[".ref($self)."] Problème fichier allHistory.xml du Manager : open: $!\n");
    $self -> setHistoryFileHandle($fileHandle);
    $self -> setHistoryFilePath($path);
    system ("chmod 644 $path");
    return 1;
}

=head2 _make

 Nom       : make
 Fonction  : Crée le dossier des logs du TaskManager, le fichier AllHistory.xml et le fichier TaskManager.log. Ce dossier contiendra également tous les logs des taches exécuter à partir de ce TaskManager.
 Exemple   : $runner -> _make();
 Retour    : Null.
 Arguments : Null.

=cut
sub _make {
    my $self = shift;
    
    $self -> _makeOutDir();
    $self -> _makeOutFile();
    $self -> _makeHistoryFile();
    
    print {$self -> getOutHandle()} TaskManager2::ManagerFactory -> _toStringWorkflowInfos();
    
    my $infosToPrint = "";
    $infosToPrint .= "###################################\n";
    $infosToPrint .= "#### ".ref($self)." ####\n";
    $infosToPrint .= "###################################\n";
    $infosToPrint .= "Logs path        : ".$self -> getOutDir()."\n";
    $infosToPrint .= "Submit logs path : ".$self -> getOutDir()."/infos.log\n";
    $infosToPrint .= "Run logs path    : ".$self -> getHistoryFilePath()."\n";
    $infosToPrint .= "Workflow Command : ".basename($0)." ".join(" ", @ARGV)."\n";
    print {$self -> getOutHandle()} $infosToPrint;
    print $infosToPrint;
}

=head2 toStringTaskInfos

 Nom       : toStringTaskInfos
 Fonction  : Renvoie une chaine de caractères décrivant une taches.
 Exemple   : my $taskInfos = $runner -> toStringTaskInfos();
 Retour    : Null.
 Arguments : Null.

=cut
sub toStringTaskInfos{
    my ($self, $task) = @_;
    
    my $taskInfos = "";
    $taskInfos .= "--------------------------------------------------------------------------------\n";
    #$taskInfos .= "\n################################################################################\n";
    $taskInfos .= $task -> getName();
    #$taskInfos .= "\n################################################################################\n";
    $taskInfos .= "\n--------------------------------------------------------------------------------\n";
    $taskInfos .= "[".ref($task)."] Job Name          : ".$task -> getName()."\n";
    if(defined $task -> getID()){
         $taskInfos .= "[".ref($task)."] Job ID            : ".$task -> getID()."\n";
    }
    my $dep = "";
    foreach(@{$task -> getDep()}){
        $dep .= $_ -> getName()."(".$_ -> getID()."); ";
    }
    $taskInfos .= "[".ref($task)."] Job Dependency    : ".$dep."\n";
    $taskInfos .= "[".ref($task)."] Executed Command  : ".$task -> getCmdToExecute()."\n";
    $taskInfos .= "[".ref($task)."] Submited File     : \n";
    $taskInfos .= "\n".$task;
    #$taskInfos .= "################################################################################\n";
    $taskInfos .= "--------------------------------------------------------------------------------\n";
    return $taskInfos;
}


sub toManagerHistoryFile {
	my ($self, $str) = @_;
	
	open my($allHistoryFile), ">>", $self -> getHistoryFilePath() or croak "open : $!\n";
    print $allHistoryFile  $str;
	close($allHistoryFile);
    return 1;
}


=head2 getDuration

 Nom       : getDuration
 Fonction  : Renvoie la durée de la tache en secondes.
 Exemple   : $status = $runner -> getDuration();
 Retour    : NUMBER.
 Arguments : Null.

=cut
sub getDuration {
    my $self = shift;
    
    # calcul temps execution
    # converti une date sous forme de chaine caractères (2014-11-17T14:24:26) en un tableau (($sec, $min, $hours, $day, $mon, $year)). 
    sub dateStringToTable{
        my $str = shift;
        my @dateTable = ();
	    my ($D, $H) = split ("T", $str);
	    push(@dateTable, split(":", $H));
	    push(@dateTable, split("-", $D));
	    my ($hours, $min, $sec, $year, $mon, $day) = @dateTable;
	    @dateTable = ($sec, $min, $hours, $day, ($mon-1), ($year-1900));
	    return \@dateTable;
    }
    
    my $secondsDif = "Unknown";
    	
    if($self -> getManagerEndTime() eq "Unknown"){
    	$self -> setManagerEndTime();
	}
    	
    if($self -> getManagerStartTime() eq "Unknown"){
   		$secondsDif = "Unknown";
   	}else{
   		$secondsDif = timelocal(@{dateStringToTable($self -> getManagerEndTime())})-timelocal(@{dateStringToTable($self -> getManagerStartTime())});
   	}
    
	return $secondsDif;
}

=head2 setWorkflowInfos()

 Nom       : setWorkflowInfos
 Fonction  : Stock les infos sur le run. Nombre de jobs, somme des temps de restitution de chaque jobs, sommes des ressources utilisées par chaque jobs, temps de restitution du workflow.
 Exemple   : $runner -> setWorkflowInfos();
 Retour    : NUMBER.
 Arguments : Null.

=cut
sub setWorkflowInfos {
    my $self = shift;
    
    $self -> _countJobsNbr();
    $self -> _countJobTime();
    $self -> _countJobsRessources();
    $self -> _countWorkflowRestitutionDuration();
    return 1;
}


=head2 getWorkflowInfos()

 Nom       : getWorkflowInfos
 Fonction  : Retourne les infos sur le workflow. Nombre de jobs, somme des temps de restitution de chaque jobs, sommes des ressources utilisées par chaque jobs, temps de restitution du workflow.
 Exemple   : $runner -> getWorkflowInfos();
 Retour    : NUMBER.
 Arguments : Null.

=cut
sub getWorkflowInfos {
    my $self = shift;
    
    if(!defined $self->{ WORKFLOW_INFOS }){
    	my %workflowInfos = ("jobsNbr" => 0, "workflowRestitutionDuration" => 0, "sumJobDuration" => 0, "sumJobRessource" => 0);
        $self->{ WORKFLOW_INFOS } = \%workflowInfos;
    }
    return $self->{ WORKFLOW_INFOS };
}

=head2 _countJobTime()

 Nom       : _countJobTime
 Fonction  : Stock la somme des temps de resitution de chaque jobs.
 Exemple   : $runner -> _countJobTime();
 Retour    : NUMBER.
 Arguments : Null.

=cut
sub _countJobTime {
    my $self = shift;
 	 
 	my $workflowInfosRef = $self->getWorkflowInfos();
 	my @tabOfAllTasks = @{$self -> getTasks()};
 	
 	my $sumOfJobDuration = 0;
 	
 	foreach(@tabOfAllTasks){
 		if($_ -> getStatus() != -1){
 			$sumOfJobDuration += $_ -> getDuration();
 		}
 	}
	
 	$workflowInfosRef -> {"sumJobDuration"} += $sumOfJobDuration;
 	
 	return $workflowInfosRef -> {"sumJobDuration"};
}

=head2 _countJobsNbr()

 Nom       : _countJobsNbr
 Fonction  : Stock le nombre de jobs completements terminés.
 Exemple   : $runner -> _countJobsNbr();
 Retour    : NUMBER.
 Arguments : Null.

=cut
sub _countJobsNbr {
    my $self = shift;
 	 
 	my $workflowInfosRef = $self->getWorkflowInfos();
 	$workflowInfosRef -> {"jobsNbr"} += int(@{$self -> getTasks()});
 	return $workflowInfosRef -> {"jobsNbr"};
}


=head2 _countJobsRessources()

 Nom       : _countJobsRessources
 Fonction  : Stock la sommes des ressources utilisées par chaque job.
 Exemple   : $runner -> _countJobsRessources();
 Retour    : NUMBER.
 Arguments : Null.

=cut
sub _countJobsRessources {
    my $self = shift;
 	 
 	my $workflowInfosRef = $self->getWorkflowInfos();
 	my $sumOfJobRessources = 0;
 	
 	foreach(@{$self -> getTasks()}){
 		if($_ -> getStatus() != -1){
 			$sumOfJobRessources += int(int($_ -> getDuration()) * int($_ -> getNbrTask()) * int($_ -> getNbrCorePerTask()));
 		}
 		
 	}
 	
 	$workflowInfosRef -> {"sumJobRessource"} += $sumOfJobRessources;
 	return $workflowInfosRef -> {"sumJobRessource"};
}

=head2 _countWorkflowRestitutionDuration()

 Nom       : _countWorkflowRestitutionDuration
 Fonction  : Stock le temps de restituion du workflow.
 Exemple   : $runner -> _countWorkflowRestitutionDuration();
 Retour    : NUMBER.
 Arguments : Null.

=cut
sub _countWorkflowRestitutionDuration {
    my $self = shift;
 	 
 	my $workflowInfosRef = $self->getWorkflowInfos();
 	
 	$workflowInfosRef -> {"workflowRestitutionDuration"} = $self -> getDuration();
 	return $workflowInfosRef -> {"workflowRestitutionDuration"};
}

=head2 infosOnWorkflow()

 Nom       : infosOnWorkflow
 Fonction  : Mets à jours les infos du run à la fin du run et retour une version imprimable.
 Exemple   : $runner -> infosOnWorkflow();
 Retour    : NUMBER.
 Arguments : Null.

=cut
sub infosOnWorkflow {
    my $self = shift;
 	my $str = "";
 	 
 	$self -> setManagerEndTime();
    $self -> setWorkflowInfos();
    my %workflowInfos = %{$self->getWorkflowInfos()};
    
    $str .=  "-----\n";
    $str .=  "Nombre total de jobs                     : ".$workflowInfos{"jobsNbr"}."\n";
    $str .=  "Temps total des runs (s)                 : ".$workflowInfos{"sumJobDuration"}."\n";
    $str .=  "Temps total des ressources reservees (s) : ".$workflowInfos{"sumJobRessource"}."\n";
    $str .=  "Temps total du workflow (s)              : ".$workflowInfos{"workflowRestitutionDuration"}."\n";
    $str .=  "-----\n";
    
    return $str;
}


=head2 reduceNumberOfJob()

 Nom       : reduceNumberOfJob
 Fonction  : Réduit le nombre de job quand il y a un trop grand nombre à soumettre en les regroupant dans un seul job.
 Exemple   : $runner -> reduceNumberOfJob(1);
 Retour    : Null si arguments présent, la valeur du bolean si argument non présent.
 Arguments : Boolean, activer la reduction du nombre de jobs 1, 0 sinon

=cut
sub reduceNumberOfJob(){
    my ($self, $boolean) = @_;
    my $old;
    
    if(!defined $self->{ REDUCE_JOB }){
    	$self->{ REDUCE_JOB } = 0;
    }
    if(defined $boolean){
    	$old = $self->{ REDUCE_JOB };
    	$self->{ REDUCE_JOB } = $boolean;
    }else{
		$old = $self->{ REDUCE_JOB };
    }
    
    return $old;
}


=head2 setLimitToGlost

 Nom       : setLimitToGlost
 Fonction  : Definit le nombre tâches à partir duquel faire une tâche GLOST.
 Exemple   : $runner -> setLimitToGlost(100);
 Retour    : NUMBER, l'ancien nombre.
 Arguments : NUMBER, le nouveau nombre.

=cut
sub setLimitToGlost {
    my ($self, $arg) = @_;
    if (! defined $arg || ($arg =~ m/\D+/)){
 		croak ("[".ref($self)."] La méthode \$runner -> setLimitToGlost() prend obligatoirement un argument sous la forme d'un nombre entier.\n"); 
    }
    my $tmp = $self -> getLimitToGlost();
    $self -> { LIMIT_TO_GLOST } = $arg;
    return $tmp;
}

=head2 getLimitToGlost

 Nom       : getLimitToGlost
 Fonction  : Retourne le nombre tâches à partir duquel faire une tâche GLOST.
 Exemple   : $runner -> getLimitToGlost();
 Retour    : NUMBER, le nombre de tâche.
 Arguments : Null.

=cut
sub getLimitToGlost {
    my $self = shift;
    if(! defined $self -> {LIMIT_TO_GLOST}){
        $self -> {LIMIT_TO_GLOST} = 120;
    }
    return $self -> { LIMIT_TO_GLOST };
}

=head2 load

 Nom       : load
 Fonction  : Charge un fichier de configuration, ou chaque job est defini de la facon suivante :
               task <jobname> : <dependance1> <dependance2> ...
               <commande>
             Chaque dependance est un nom de job. Une ligne commencant par # est ignoree.
 Exemple   : $run->load($filename);
 Retour    : aucun.
 Arguments : une chaine de caracteres (nom de fichier).

=cut
sub load {
    my($self, $file) = @_;
    my $fhconf = new FileHandle($file, "r");
    if (!defined $fhconf) { die "Impossible d'ouvrir le fichier de configuration : $file.\n"; }
    my $ligne = $fhconf->getline();
    chomp($ligne);
    my $continue = 1;
    
    while($continue) {
		if ($ligne =~ /\#/i || $ligne =~ /^[\e|\t]*$/i) {
		    $ligne = $fhconf->getline();
		    chomp($ligne);
		    next;
		}
		if ($ligne =~ /^task\s+([A-Za-z0-9]+)\s+:\s*(.*)$/) {
		    my $name = $1;
		    if ($self->existTask($name)) { 
				die "[TASKNAME]Erreur l.$. : la tache $name existe deja.\n";
		    }
		    my $dep = $2 || "";
		    my @dependance = ();
		    while(!$dep =~ /^\s*$/) {
				$dep =~ /^\s*([A-Za-z0-9]+)(.*)$/;
				if (! $self->existTask($1)) { 
				    die "[DEPENDANCE]Erreur l.$. : la tache $1 n'existe pas.\n";
				}
				push(@dependance, $self-> getTask($1));
				$dep = $2;
		    }
		    
		    my $command = "";
		    while($ligne = $fhconf->getline()) {
				chomp($ligne);
				if ($ligne =~ /\#/i || $ligne =~ /^\s*$/i) {
				    next;
				}
				if (!($ligne =~ /^task\s+[A-Za-z0-9]+\s+:\s*.*$/)) {
				    $command .= $ligne;
				} else {
				    last;
				}
		    }

			my $task = $self -> createJob($name, $command);
			$task -> addDep(\@dependance);
		    
		    if (! defined $ligne) { $continue = 0; }
		} else {
		    $continue = 0;
		}
    }
}


########################################################################################################################################################
## Methodes pour la compatibilité avec les scripts existants et utilisant les methodes du ForkManager.
########################################################################################################################################################
=head2 logFile

 Nom       : logFile
 Fonction  : Definit le handle pour les logs de TaskManager.
 Exemple   : $runner -> logFile($handle);
 Retour    : STRING, l'ancien handle.
 Arguments : STRING, le handle pour les logs du BatchManager.

=cut
sub logFile(){
    my ($self, $handle) = @_;
    return $self-> setOutHandle($handle);
}

=head2 logDir

 Nom       : logDir
 Fonction  : Permet de définir le path du dossier OUT du wrapper. Dans ce dossier sont stockés tous les scripts run.sh, les sorties STDOUT et STDERR du TaskManager.
 Exemple   : $runner -> logDir("path/du/dossier/temporaire/du/pipeline");
 Retour    : STRING, l'ancien path.
 Arguments : STRING, le path du dossier OUT du wrapper.

=cut
sub logDir(){
    my ($self, $path) = @_;
    unless ( -d $path ) {
        unless ( mkpath($path) ) {
            if ($@) {
                print "[".ref($self)."] Impossible de créer $path: $@";
            }
        }
    }
    return $self-> setOutDir($path);
}

=head2 set_max_process

 Nom       : set_max_process
 Fonction  : Definit le nombre de processus en parallel au maximun pour le wrapper.
 Exemple   : $runner -> set_max_process(100);
 Retour    : NUMBER, l'ancien nombre de precessus.
 Arguments : NUMBER, le nombre de processus à utiliser au maximun.

=cut
sub set_max_process(){
    my ($self, $nbrProc) = @_;
    return $self -> setMaxProc($nbrProc);
}

=head2 get_tasks

 Nom       : get_tasks
 Fonction  : Renvoie l'ensemble des taches associées au wrapper.
 Exemple   : $runner -> get_tasks();
 Retour    : (Référence), la référence vers le tableau des taches à manager (tableau de références pointants vers les objects Task).
 Arguments : Null

=cut
sub get_tasks(){
    my $self = shift;
    return $self -> getTasks();
}

sub run();

1;