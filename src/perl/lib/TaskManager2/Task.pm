################################################################################
#
# * $Id: Task.pm,v 0.1 2014/04/2 akourlai Exp $
#
# * Task module for TaskManager2::Task.pm
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

TaskManager2::Task.pm - Class ABSTRAITE qui gère les fonctionalités communes des taches.

=head1 SYNOPSIS


=head1 DESCRIPTION

    
=head1 AUTHOR

N'hésitez pas à me contacter pour tout renseignement, ajout de fonctionalité ou signalement de bugs.

Artem Kourlaiev, akourlai@genoscope.cns.fr

=cut

package TaskManager2::Task;

use overload '""'  =>  \&_display;
use strict;
use warnings;

use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

use File::Temp qw(tempdir tempfile);
use Time::Local qw(timelocal);
use Carp;

require Exporter;
our @ISA = qw();
# Aucun export par defaut
@EXPORT = qw();
# Aucun export autorisé
@EXPORT_OK = qw();

$VERSION = '0.1';

=head2 new

 Nom       : new
 Fonction  : Crée un objet de la class Task.
 Exemple   : ABSTRACT CLASS, my $self = $class -> SUPER::new($taskName, $cmd);
 Retour    : une référence sur un objet de la class Task.
 Arguments : STRING STRING. $taskName, $cmd

=cut
sub new {
    my $class = shift;
    $class = ref($class) || $class;
    my $self = {};
    bless($self, $class);
    
    my ($taskName, $cmd) = @_;
    $self -> setName($taskName);
    $self -> setCmd($cmd);
    
    return $self;
}

##########
## SETEURS
##########

=head2 setName

 Nom       : setName
 Fonction  : Permet de définir le nom de la tache.
 Exemple   : $task -> setName("nomDeLaTache");
 Retour    : STRING, ancien nom de la tache.
 Arguments : STRING, nom de la tache.

=cut
sub setName {
    my ($self, $arg) = @_;
    if (! defined $arg){
		die "La methode \$task -> setName() prend obligatoirement un argument.\n"; 
	}
    my $tmp = $self->getName();
    my @a = split(" ", $arg);
    my $str = join("_", @a);
    
    $self->{ NAME } = $str;
    return $tmp;
}

=head2 setCmd

 Nom       : setCmd
 Fonction  : Permet de charger la commande dans l'objet Task.
 Exemple   : $task -> setCmd("MaCommande");
 Retour    : STRING, l'ancienne commande.
 Arguments : STRING, une commande.

=cut
sub setCmd {
    my ($self, $arg) = @_;
    if (! defined $arg){
		die "La methode \$task -> setCmd() prend obligatoirement un argument. (".$self -> getName().")\n";
	}
    my $tmp = $self->getCmd();
    $self->{ CMD } = $arg;
    return $tmp;
}

=head2 setCmdToExecute

 Nom       : setCmdToExecute
 Fonction  : Permet de charger la commande exécuté dans l'objet Task.
 Exemple   : $task -> setCmdToExecute("MaCommande");
 Retour    : STRING, l'ancienne commande.
 Arguments : STRING, une commande.

=cut
sub setCmdToExecute {
    my ($self, $arg) = @_;
    if (! defined $arg){
		die "La methode \$task -> setCmdToExecute() prend obligatoirement un argument.\n"; 
	}
    my $tmp = $self->getCmdToExecute();
    $self->{ CMD_TO_EXECUTE } = $arg;
    return $tmp;
}

=head2 setOutDir

 Nom       : setOutDir
 Fonction  : Définit le path du dossier de la tache.
 Exemple   : $task -> setOutDir("Chemin/du/dossier/de/la/tache/");
 Retour    : STRING, le path du dossier de la tache.
 Arguments : STRING, le path du dossier où créer le dossier de la tache.

=cut
sub setOutDir {
    my ($self, $arg) = @_;
    if (! defined $arg){
		die "La methode \$task -> setOutDir() prend obligatoirement un argument.\n"; 
	}
    my $tmp = $self -> getOutDir();
    $self->{ OUT_DIR } = $arg;
    return $tmp;
}

=head2 setID

 Nom       : setID
 Fonction  : Associe le JobID de la tache.
 Exemple   : $task->setID("JobID");
 Retour    : L'ancien jobID.
 Arguments : STRING, le JobID.

=cut
sub setID {
    my ($self, $arg) = @_;
    if (! defined $arg){
		die "La methode \$task -> setID() prend obligatoirement un argument.\n"; 
	}
	my $jobID = $self -> getID();
    $self->{ ID } = $arg;
    return $jobID;
}

=head2 setSubmitTime

 Nom       : setSubmitTime
 Fonction  : Associe le moment de soumission à la tache.
 Exemple   : $task -> setSubmitTime();
           : $task -> setSubmitTime("2014-06-30T10:18:06");
 Retour    : L'ancien moment.
 Arguments : Null ou STRING.

=cut
sub setSubmitTime {
    my ($self, $date) = @_;
    
    my $oldTime = $self -> getSubmitTime();
    if(!defined $date){
    	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
        my $time =  sprintf("%04d-%02d-%02dT%02d:%02d:%02d",1900+$year,$mon+1,$mday,$hour,$min,$sec);
        $self->{ SUBMIT_TIME } = $time;
    }else{
        $self->{ SUBMIT_TIME } = $date;
    }
    return $oldTime;
}

=head2 setBegin

 Nom       : setBegin
 Fonction  : Memorise le début de la tache.
 Exemple   : $task -> setBegin();
           : $task -> setBegin("2014-06-30T10:18:06");
 Retour    : L'ancien moment.
 Arguments : Null ou STRING.

=cut
sub setBegin {
 my ($self, $date) = @_;
    
    my $oldTime = $self -> getBegin();
    if(!defined $date){
    	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
        my $time =  sprintf("%04d-%02d-%02dT%02d:%02d:%02d",1900+$year,$mon+1,$mday,$hour,$min,$sec);
        $self->{ BEGIN_TIME } = $time;
    }else{
        $self->{ BEGIN_TIME } = $date;
    }
    return $oldTime;
}

=head2 setEnd

 Nom       : setEnd
 Fonction  : Memorise la fin de la tache.
 Exemple   : $task -> setEnd();
           : $task -> setEnd("2014-06-30T10:18:06");
 Retour    : L'ancien moment.
 Arguments : Null ou STRING. 

=cut
sub setEnd {
    my ($self, $date) = @_;
    
    my $oldTime = $self -> getEnd();
    if(!defined $date){
    	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
        my $time =  sprintf("%04d-%02d-%02dT%02d:%02d:%02d",1900+$year,$mon+1,$mday,$hour,$min,$sec);
        $self->{ END_TIME } = $time;
    }else{
        $self->{ END_TIME } = $date;
    }
    return $oldTime;
}

=head2 setReturn

 Nom       : setReturn
 Fonction  : Memorise le code retour de la tache.
 Exemple   : $task -> setReturn("codeRetour");
 Retour    : Null.
 Arguments : Le code retour de la tache.

=cut
sub setReturn {
    my ($self, $arg) = @_;
    if (! defined $arg) { die "La methode \$task -> setReturn() prend obligatoirement un argument.\n"; }
    my $tmp = $self->getReturn();
    $self->{ RETURN } = $arg;
    return $tmp;
}

=head2 setHistoryFileHandle

 Nom       : setHistoryFileHandle
 Fonction  : Associe le handle du fichier history à la tache.
 Exemple   : $task -> setHistoryFileHandle($handle);
 Retour    : STRING, handle de l'ancien fichier history.
 Arguments : STRING, le handle du fichier history.

=cut
sub setHistoryFileHandle {
    my ($self, $arg) = @_;
    if (! defined $arg){
		die "La methode \$task -> setHistoryFile() prend obligatoirement un argument.\n"; 
	}
    my $tmp = $self->getHistoryFileHandle();
    $self->{ HISTORY_FILE_HANDLE } = $arg;
    return $tmp;
}

=head2 setHistoryFilePath

 Nom       : setHistoryFilePath
 Fonction  : Definit le path pour le fichier history.xml de la tache.
 Exemple   : $task -> setHistoryFilePath($path);
 Retour    : REF, l'ancien path.
 Arguments : REF, le path du le fichier history.xml de la tache.

=cut
sub setHistoryFilePath {
    my ($self, $arg) = @_;
    if (! defined $arg){
 		die "La methode \$task -> setHistoryFilePath() prend obligatoirement un argument.\n"; 
    }
    my $tmp = $self -> getHistoryFilePath();
    $self->{ PATH_HISTORY } = $arg;
    return $tmp;
}

=head2 setStatus

 Nom       : setStatus
 Fonction  : Definit le status de la tache : -1 non soumise, 0 en cours d'exécution, 1 terminé.
 Exemple   : $task -> setStatus(1);
 Retour    : NUMBER, l'ancien status.
 Arguments : NUMBER, le status.

=cut
sub setStatus {
    my ($self, $arg) = @_;
    if (! defined $arg){
 		die "[".ref($self)."] La methode \$runner -> setStatus() prend obligatoirement un argument sous la forme d'un nombre entier.\n"; 
    }
    my $tmp = $self -> getStatus();
    $self -> { STATUS } = $arg;
    return $tmp;
}

=head2 setDep

 Nom       : setDep
 Fonction  : Change les dépandaces de la tâche.
 Exemple   : $task -> setDep(\@dep);
 Retour    : REF, tableau des dépendances effacées.
 Arguments : REF, tableau des dépendances à ajouter.

=cut
sub setDep {
    my ($self, $task) = @_;
    if (! defined $task || @_ > 2){
	    die "La methode \$task -> setDep() prend obligatoirement un argument (un objet Task ou une reference vers un tableau d'objet Task).\n";
	}
	my $tmp = $self -> getDep();
	$self->{ DEP } = undef;
	
	if(ref($task) eq 'ARRAY'){
	    foreach(@{$task}){
	        my $task = $_;
	        push(@{$self -> getDep()}, $task);
	    }
	}else{
	    push(@{$self -> getDep()}, $task);
	}
    return $tmp;
}

=head2 setState

 Nom       : setState
 Fonction  : Definit l'état du job Slurm.
 Exemple   : $task -> setState("SUBMITED");
 Retour    : STRING, l'ancien état.
 Arguments : STRING, l'état à definirs.

=cut
sub setState {
    my ($self, $arg) = @_;
    my $tmp = $self -> getState();
    $self->{ STATE } = $arg;
    return $tmp;
}

##########
## GETEURS
########## 

=head2 getName

 Nom       : getName
 Fonction  : Renvoie le nom de la tache.
 Exemple   : $task -> getName();
 Retour    : STRING, le nom de la tache.
 Arguments : Null.

=cut
sub getName {
    my $self = shift;
    return $self->{ NAME };
}
# pour compatibilité avec le passif
sub get_name {
	my $self = shift;
	return $self -> getName();
}


=head2 getCmd

 Nom       : getCmd
 Fonction  : Renvoie la commande de la tache.
 Exemple   : $task -> getCmd();
 Retour    : STRING, la commande.
 Arguments : Null.

=cut
sub getCmd {
    my $self = shift;
    return $self -> { CMD };
}
# pour compatibilité avec le passif
sub get_command {
	my $self = shift;
	return $self -> getCmd();
}
# pour compatibilité avec le passif
sub get_return {
	my $self = shift;
	return $self -> getReturn();
}
# pour compatibilité avec le passif
sub get_begin {
	my $self = shift;
	return $self -> getBegin();
}
# pour compatibilité avec le passif
sub get_end {
	my $self = shift;
	return $self -> getEnd();
}

=head2 getCmdToExecute

 Nom       : getCmdToExecute
 Fonction  : Renvoie la commande de la tache.
 Exemple   : $task -> getCmdToExecute();
 Retour    : STRING, la commande.
 Arguments : Null.

=cut
sub getCmdToExecute {
    my $self = shift;
    return $self -> { CMD_TO_EXECUTE };
}

=head2 getOutDir

 Nom       : getOutDir
 Fonction  : Permet de définir le dossier de la tache.
 Exemple   : $task -> getOutDir();
 Retour    : STRING, le chemin du dossier de la tache.
 Arguments : Null.

=cut
sub getOutDir {
    my $self = shift;
    if(!defined $self -> {OUT_DIR}){
        $self -> {OUT_DIR} = "./";
    }
    return $self -> {OUT_DIR};
}

=head2 getFileRunPath

 Nom       : getFileRunPath.
 Fonction  : Renvoie le path du fichier temporaire de la tache.
 Exemple   : $task -> getFileRunPath();
 Retour    : (STRING), le chemin de la tache.
 Arguments : Null.

=cut
sub getFileRunPath {
    my $self = shift;
    if(!defined $self->{ OUT_FILE_PATH }){
        $self->{ OUT_FILE_PATH } = "";
    }
    return $self->{ OUT_FILE_PATH };
}

=head2 getFileRunHandle

 Nom       : getFileRunHandle
 Fonction  : Renvoie le handle du fichier de la tache.
 Exemple   : $task -> getFileRunHandle();
 Retour    : (handle), le handle de la tache.
 Arguments : Null.

=cut
sub getFileRunHandle {
    my $self = shift;
    return $self->{ OUT_FILE_HANDLE };
}

=head2 getID

 Nom       : getID
 Fonction  : Renvoie le JobID associé à la tache.
 Exemple   : $task -> getID();
 Retour    : (STRING), le jobID associé à la tache.
 Arguments : Null.

=cut
sub getID {
    my $self = shift;
    if(!defined $self->{ ID }){
        $self->{ ID } = "ID no Affected";
    }
    return $self->{ ID };
}

=head2 getDep

 Nom       : getDep
 Fonction  : Renvoie le(s) dependance(s) associée(s) à la tache.
 Exemple   : $task -> getDep();
 Retour    : (Référence) tab TASK, la référence vers le tableau de dépendance(s) associé(s) à la tache.
 Arguments : Null.

=cut
sub getDep {
    my $self = shift;
    if(!defined $self->{ DEP }){
        my @tabDep = ();
        $self->{ DEP } = \@tabDep;
    }
    return $self->{ DEP };
}

=head2 getSubmitTime

 Nom       : getSubmitTime
 Fonction  : Renvoie le moment de soumission de la tache.
 Exemple   : $task -> getSubmitTime();
 Retour    : STRING, le moment de soumission (YYYY-MM-DD HH:MM:SS)
 Arguments : Null.

=cut
sub getSubmitTime {
    my $self = shift;
    if(!defined $self->{ SUBMIT_TIME }){
        $self->{ SUBMIT_TIME } = "Tache non soumise\n"
    }
    return $self->{ SUBMIT_TIME };
}

=head2 getBegin

 Nom       : getBegin
 Fonction  : Renvoie la date du début de la tache.
 Exemple   : $task -> getBegin();
 Retour    : L'ancien moment.
 Arguments : Null.

=cut
sub getBegin {
    my $self = shift;
    if(! defined $self->{ BEGIN_TIME }){
    	$self->{ BEGIN_TIME } = "";
    }
    return $self -> { BEGIN_TIME };
}

=head2 getEnd

 Nom       : getEnd
 Fonction  : Renvoie la date de la fin de la tache.
 Exemple   : $task -> getEnd();
 Retour    : L'ancien moment.
 Arguments : Null.

=cut
sub getEnd {
    my $self = shift;
    if(! defined $self->{ END_TIME }){
    	$self->{ END_TIME } = "";
    }
    return $self->{ END_TIME };
}

=head2 getReturn

 Nom       : getReturn
 Fonction  : Renvoie le code retour de la tache et undef si elle n a pas encore été executée.
 Exemple   : $ret = $task->getReturn();
 Retour    : NUMBER ou undef. code retour.
 Arguments : aucun.

=cut
sub getReturn {
    my $self = shift;
    if(! defined $self->{ RETURN }){
    	$self->{ RETURN } = "";
    }
    
    return $self->{ RETURN };
}

=head2 getHistoryFileHandle

 Nom       : getHistoryFileHandle
 Fonction  : Renvoie le handle du fichier history.
 Exemple   : my $handle = $task -> getHistoryFileHandle();
 Retour    : REF, le handle du fichier history.
 Arguments : Null.

=cut
sub getHistoryFileHandle {
    my $self = shift;
    return $self->{ HISTORY_FILE_HANDLE };
}

=head2 getHistoryFilePath

 Nom       : getHistoryFilePath
 Fonction  : Renvoie le path du fichier history.xml de la tache.
 Exemple   : $task -> getHistoryFilePath();
 Retour    : REF, le path du fichier history.xml de la tache.
 Arguments : Null.

=cut
sub getHistoryFilePath {
    my $self = shift;    
    if(!defined $self -> { PATH_HISTORY }) {
        $self -> { PATH_HISTORY } = "";
    }
    return $self->{ PATH_HISTORY };
}

=head2 getStatus

 Nom       : getStatus
 Fonction  : Renvoie le le status de la tache : -1 non exécuté, 0 en exécution, 1 terminé.
 Exemple   : $status = $task -> getStatus();
 Retour    : NUMBER.
 Arguments : Null.

=cut
sub getStatus {
    my $self = shift;
    
    if(!defined $self -> { STATUS }){
        $self -> { STATUS } = -1;
    }
    return $self -> { STATUS };
}

=head2 getDuration

 Nom       : getDuration
 Fonction  : Renvoie la durée de la tache en secondes.
 Exemple   : $status = $task -> getDuration();
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
    
    try{
    	my $secondsDif = "Unknown";
    	
    	if($self -> getEnd() eq "Unknown"){
    		$self -> setEnd();
    	}
    	
    	if($self -> getBegin() eq "Unknown"){
    		$secondsDif = "Unknown";
    	}else{
    		$secondsDif = timelocal(@{dateStringToTable($self -> getEnd())})-timelocal(@{dateStringToTable($self -> getBegin())});
    	}
    	
    	return $secondsDif;
    }catch{
    	die "Erreur dans la tache : ".$self -> getName()."\n";
    }
}

=head2 getModules

 Nom       : getModules
 Fonction  : Renvoie le(s) module(s) associé(s) à la tache.
 Exemple   : $task->getModules();
 Retour    : (Référence), la référence vers le tableau des modules associés à la tache.
 Arguments : Null.

=cut
sub getModules {
    my $self = shift;
    if(!defined $self->{ MODULES }){
        my @tabModules = ();
        $self->{ MODULES } = \@tabModules;
    }
    return $self->{ MODULES };
}

=head2 getState

 Nom       : getState
 Fonction  : Renvois le dernier état conne du job Slurm.
 Exemple   : $task -> getState();
 Retour    : STRING, l'état du job.
 Arguments : Null.

=cut
sub getState {
    my $self = shift;
    
    if(!defined $self -> { STATE }){
        $self -> { STATE } = "Unknown";
    }
    return $self -> { STATE };
}

=head2 getNbrTask

 Nom       : getNbrTask
 Fonction  : Renvoie le nombre de noeuds à résérver.
 Exemple   : $task -> getNbrTask();
 Retour    : NOMBRE, le nombre de noeud à résérver.
 Arguments : Null.

=cut
sub getNbrTask {
    my $self = shift;
    if(!$self->{ NBR_TASK }){
    	$self->{ NBR_TASK } = 1;
    }
    return $self->{ NBR_TASK };
}

=head2 getNbrCorePerTask

 Nom       : getNbrCorePerTask
 Fonction  : Renvoie le nombre de coeur par noeud à résérver.
 Exemple   : $task -> getNbrCorePerTask();
 Retour    : NOMBRE, le nombre de coeur par noeud à résérver.
 Arguments : Null.

=cut
sub getNbrCorePerTask {
    my $self = shift;
    if(!$self->{ CORE_PER_TASK }){
    	$self->{ CORE_PER_TASK } = 1;
    }
    return $self->{ CORE_PER_TASK };
}

=head2 setNbrTask

 Nom       : setNbrTask
 Fonction  : Permet définir les nombre de noeuds à résérver pour la tache.
 Exemple   : $task -> setNbrTask(5);
 Retour    : NOMBRE, l'ancienne nombre de noeuds.
 Arguments : NOMBRE, le nombre noeuds à résérver pour la tache.

=cut
sub setNbrTask {
    my ($self, $arg) = @_;
    if (! defined $arg){
		die "La methode \$task -> setNbrTask() prend obligatoirement un argument.\n"; 
	}
    my $tmp = $self->getNbrTask();
    $self->{ NBR_TASK } = $arg;
    return $tmp;
}

=head2 setNbrCorePerTask

 Nom       : setNbrCorePerTask
 Fonction  : Permet définir les nombre de coeurs à résérver par noeud pour une tache.
 Exemple   : $task -> setNbrCorePerTask(16);
 Retour    : NOMBRE, l'ancienne nombre de coeurs par noeud.
 Arguments : NOMBRE, nombre de coeurs par noeud pour la tache.

=cut
sub setNbrCorePerTask {
    my ($self, $arg) = @_;
    if (! defined $arg){
		die "La methode \$task -> setNbrCorePerTask() prend obligatoirement un argument.\n"; 
	}
    my $tmp = $self->getNbrCorePerTask();
    $self->{ CORE_PER_TASK } = $arg;
    return $tmp;
}

############
## METHODES
############ 

=head2 addDep

 Nom       : addDep
 Fonction  : Rajoute une ou des dependances à la tache.
 Exemple   : $task -> addDep($task);
           : $task -> addDep(\@task);
 Retour    : Null
 Arguments : OBJECT ou REF, la tache ou une reference vers un tableau de taches.

=cut
sub addDep {
    my ($self, $task) = @_;
    if (! defined $task || @_ > 2){
	    croak ("[".$self-> getName()."] La methode \$task -> addDep() prend obligatoirement un argument.");
	}
	if(ref($task) eq 'ARRAY'){
	    foreach(@{$task}){
	        my $task = $_;
	        push(@{$self -> getDep()}, $task);
	    }
	}else{
	    push(@{$self -> getDep()}, $task);
	}
}

=head2 delDep

 Nom       : delDep
 Fonction  : Supprime une ou des dependances à la tache.
 Exemple   : $task -> delDep($task); 
           : $task -> delDep(\@task);
 Retour    : Null
 Arguments : OBJECT ou REF, la tache ou une reference vers un tableau de taches.

=cut

sub delDep {
    my ($self, $task) = @_;
    if (! defined $task || @_ > 2){
	    die "[".$self-> getName()."] La methode \$task -> addDep() prend obligatoirement un argument (un objet Task ou une reference vers un tableau d'objet Task).\n";
	}
	if(ref($task) eq 'ARRAY'){
	    foreach(@{$task}){
	        my $task = $_;
	        foreach(@{$self -> getDep()}){
	            my $taskDep = $_;
	            if($taskDep and ref($taskDep) and $task and ref($task) and refaddr($taskDep) == refaddr($task)){
	                print "Ok del";
	                print $taskDep -> getName();
	            }
	        }
	        push(@{$self -> getDep()}, $task);
	    }
	}else{
	    push(@{$self -> getDep()}, $task);
	}
	
}

=head2 make

 Nom       : make
 Fonction  : Crée le dossier tmp résérvé à la tache, enregistre le script bash. Les sorties STDOUT et STDERR y serrons enregistrées après la soumission.
 Exemple   : $task -> make();
 Retour    : Null.
 Arguments : Null.

=cut
sub make {
    my $self = shift;
    $self -> makeOutDir();
    $self -> makeRunFile();
    $self -> makeHistoryFile();
    my $fileHandle =  $self -> getFileRunHandle();
    print ($fileHandle $self); # ecrit la commande dans le fichier run.sh
    close($self -> getFileRunHandle());
}

=head2 makeOutDir

 Nom       : makeOutDir
 Fonction  : Crée le dossier de la tache.
 Exemple   : $task -> makeOutDir();
 Retour    : Null.
 Arguments : Null.

=cut
sub makeOutDir {
    my $self = shift;
    
	#my $dir = tempdir( $self -> getName()."_XXXX", DIR => $self -> getOutDir(), CLEANUP => 0 );# CLEANUP permet de créer/supprimer le dossier temporaire à la fin de l'exécution ou lors de la mort du pipeline.pl
    my $taskPath = $self -> getOutDir()."/".$self -> getName();
    mkdir ($taskPath) or die ("[".ref($self)."] Erreur creation repertoire de la tache ".$self -> getName()." (".$taskPath.") : $!\n");
    my $tmp = $self -> getOutDir();
    $self -> setOutDir($taskPath);
    
    system ("chmod 755 $taskPath");
    
    return $tmp;
}

=head2 makeRunFile

 Nom       : makeRunFile
 Fonction  : Permet de créer le fichier run.sh de la tache dans le dossier de la tache.
 Exemple   : $task -> makeRunFile();
 Retour    : Null.
 Arguments : Null.

=cut
sub makeRunFile {
    my $self = shift;
    
    my $fileHandle;
    my $filePath = $self -> getOutDir()."/".$self -> getName().".sh";
    open($fileHandle,">>".$filePath) or die("Probleme fichier run.sh ($self -> getName()): open: $!\n");
   	my $tmpFilePath = $self -> getFileRunPath();
	my $tmpFileHandle = $self -> getFileRunHandle();
	
    $self->{ OUT_FILE_PATH } = $filePath;
    $self->{ OUT_FILE_HANDLE } = $fileHandle;
    
    system ("chmod 744 $filePath");
    
    return ($tmpFileHandle, $tmpFilePath);
}

=head2 makeHistoryFile

 Nom       : makeHistoryFile
 Fonction  : Permet de créer le fichier history.xml de la tache.
 Exemple   : $task -> makeHistoryFile();
 Retour    : Null.
 Arguments : Null.

=cut
sub makeHistoryFile {
    my $self = shift;
    
    my $fileHandle;
    my $filePath = $self -> getOutDir()."/history.xml";
    open($fileHandle,">>".$filePath) or die("Probleme fichier history.xml de la tache ($self -> getName()): open: $!\n");
    $self -> setHistoryFileHandle($fileHandle);
    $self -> setHistoryFilePath($filePath);
    
    system ("chmod 644 $filePath");
    
}

=head2 addModule

 Nom       : addModule
 Fonction  : Associe un module à la tache. ("module load nomDuModule" dans le script bash).
 Exemple   : $task -> addModule("module load nomDuModule");
 Retour    : Null
 Arguments : STRING, nom du module.

=cut
sub addModule {
    my ($self, $nomDuModule) = @_;
    if (! defined $nomDuModule){
	    croak "[".$self-> getName()."] La methode \$task -> addModule prend obligatoirement un argument.\n";
	}
	push(@{$self->getModules()}, $nomDuModule);
}

=head2 toHistoryFile

 Nom       : toHistoryFile
 Fonction  : PRIVATE. Ecrit les informations de la tache dans history.xml et allHistory.xml.
 Exemple   : $self -> toHistoryFile();
 Retour    : Null.
 Arguments : Null.

=cut
sub historyFileToString{
    my $self = shift;
    
    my $taskStr = "";
    $taskStr .= "\t<TASK id='".$self -> getID()."' name='".$self -> getName()."' taskType = '".ref($self)."' submitTime='".$self -> getSubmitTime()."'>\n";
    $taskStr .= "\t\t<CMD><![CDATA[\n\t\t\t".$self -> getCmd()."\n\t\t]]></CMD>\n";
    my $taskDepStr = "";
    foreach(@{$self -> getDep()}){
        my $self = $_;
        $taskDepStr .= "\t\t<DEPENDENCY id='".$self -> getID()."' name='".$self -> getName()."'/>\n";
    }
    $taskStr .= $taskDepStr;
    $taskStr .= "\t\t<START>\n\t\t\t".$self -> getBegin()."\n\t\t</START>\n";
    $taskStr .= "\t\t<FINISH>\n\t\t\t".$self -> getEnd()."\n\t\t</FINISH>\n";
    $taskStr .= "\t\t<DURATION unit=\'seconds\'>\n\t\t\t".$self -> getDuration()."\n\t\t</DURATION>\n";
    $taskStr .= "\t\t<EXIT>\n\t\t\t".$self -> getReturn();
    if($self -> getState() ne "Unknown"){
   		$taskStr .= " (".$self -> getState().")";
    }
    $taskStr .= "\n\t\t</EXIT>\n\t</TASK>\n";
    
    return $taskStr;
}

sub toTaskHistoryFile{
	my ($self, $str) = @_;
	
	open my($historyFile), ">>", $self -> getHistoryFilePath() or croak "open : $!\n";
    print $historyFile  $str;
	close($historyFile);
    return 1;
}

=head2 _display

 Nom       : _display
 Fonction  : Permet d'afficher la tache (le fichier run.sh)
 Exemple   : $task -> _display();
 Retour    : STRING, contenu du fichier run.sh
 Arguments : Null.

=cut
sub _display;

1;