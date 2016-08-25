################################################################################
#
# * $Id: TaskSlurm.pm,v 0.1 2014/04/2 akourlai Exp $
#
# * TaskSlurm module for TaskManager2::TaskSlurm.pm
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

TaskManager2::TaskSlurm.pm - Class ABSTRAITE, hérite de la class TASK et regroupe les fonctionnalités commune à SLURM.

=head1 SYNOPSIS

    NE PAS UTILISER DIRECTEMENT, PASSER PAR TaskManager2::TaskFactory.pm pour créer des taches !!!
    
=head1 DESCRIPTION

    NE PAS UTILISER DIRECTEMENT, PASSER PAR TaskManager2::TaskFactory.pm pour créer des taches !!!

=head1 AUTHOR

N'hésitez pas à me contacter pour tout renseignement, ajout de fonctionalité ou signalement de bugs.

Artem Kourlaiev, akourlai@genoscope.cns.fr

=cut

package  TaskManager2::TaskSlurm;

use  TaskManager2::Task;
our @ISA = qw( TaskManager2::Task );

use strict;
use overload '""'  =>  \&_display;

=head2 new

 Nom       : new
 Fonction  : Crée un objet de la class TaskSlurm, class fille de Task. Pour des wrappers susceptibles d'être exécutés dans différents environnement (CNS, CNG, CCRT) utiliser le module TaskManager2::TaskFactory pour créer des taches.
 Exemple   : $task = TaskManager2::TaskSlurm -> new($nomDelaTache, $cmd, $nbr_noeud, $nbr_coeur_par_noeud, $queue, $groupe, $ramParNoeud);
 Retour    : une référence sur un objet de la class TaskSlurm.
 Arguments : STRINGS. $nomDelaTache, $cmd, $nbr_noeud, $nbr_coeur_par_noeud, $queue, $groupe, $ramParNoeud.

=cut
sub new {
    my $class = shift;
    my ($name, $cmd, $nbrTask, $nbrCorePerTask, $mem_limit, $time_limit, $exculisive, $queue, $projid, $qos, $extra_parameters, $starttime, $mailopts) = @_;
    #la ligne suivante appelle le constructeur de Task
    my $self = $class -> SUPER::new($name, $cmd);
    bless( $self, $class);#lie la référence à la classe courante
    
    $self -> setNbrTask($nbrTask);
    $self -> setNbrCorePerTask($nbrCorePerTask);
    $self -> setQueue($queue);
    $self -> setProjId($projid);
    $self -> setRam($mem_limit);
    
    ##
    $self -> setTimeLimit($time_limit);
    $self -> setExclusive($exculisive);
    $self -> setQos($qos);
    $self -> setExtraParameters($extra_parameters);
    $self -> setStartTime($starttime);
    $self -> setMailOpts($mailopts);
    
    return $self;
}

##########
## SETEURS
##########

=head2 setOptsTask

 Nom       : setOptsTask
 Fonction  : Enregistre les options de la tache Slurm
 Exemple   : $task -> setOptsTask("-n 5 -c 1");
 Retour    : STRING, anciennes options 
 Arguments : STRING, nouvelles options

=cut
sub setOptsTask {
    my ($self, $arg) = @_;
    if (! defined $arg){
		die "La méthode \$task -> setOptsTask() prend obligatoirement un argument."; 
	}
    my $tmp = $self->getOptsTask();
    $self->{ OPTS_TASK } = $arg;
    return $tmp;
}

=head2 setRam

 Nom       : setRam
 Fonction  : Permet de définir la quanitité de ram par coeur à utiliser, en Mbytes.
 Exemple   : $task -> setRam(7000);
 Retour    : NUMBRE, l'ancien ram alloué par coeur.
 Arguments : NUMBER, la quatité de ram par coeur à résérver à la tache.

=cut
sub setRam {
    my ($self, $arg) = @_;
    my $tmp = $self -> getRam();
    $self->{ RAM } = $arg;
    return $tmp;
}

=head2 setQueue

 Nom       : setQueue
 Fonction  : Permet de définir la queue à utiliser.
 Exemple   : $task -> setQueue("nomDeLaQueue");
 Retour    : STRING, nom de l'ancienne queue.
 Arguments : STRING, nom de la queue.

=cut
sub setQueue {
    my ($self, $arg) = @_;
    if (! defined $arg){
		die "La méthode \$task -> setQueue() prend obligatoirement un argument."; 
	}
    my $tmp = $self->getQueue();
    $self->{ QUEUE } = $arg;
    return $tmp;
}

=head2 setProjId

 Nom       : setProjId
 Fonction  : Permet de définir le groupe à utiliser.
 Exemple   : $task -> setProjId("MonGroupeAuCCRT");
 Retour    : STRING, l'ancien groupe.
 Arguments : STRING, le nom du groupe ("fg0017", ...).

=cut
sub setProjId {
    my ($self, $arg) = @_;
    if (! defined $arg){
		die "La méthode \$task -> setProjId prend obligatoirement un argument."; 
	}
    my $tmp = $self->getProjId();
    $self->{ GROUPE } = $arg;
    return $tmp;
}

=head2 setTimeLimit

 Nom       : setTimeLimit
 Fonction  : Permet de définir le temps max de la tache, en minutes.
 Exemple   : $task -> setTimeLimit("120");
 Retour    : INT, l'ancien temps max.
 Arguments : INT, le nombre de minutes max pour la tache.

=cut
sub setTimeLimit {
    my ($self, $arg) = @_;
    my $tmp = $self->getTimeLimit();
    $self->{ TIME_LIMITE } = $arg;
    return $tmp;
}

=head2 setExclusive

 Nom       : setExclusive
 Fonction  : Permet de forcer l'usage exculsive d'un noeud pour la tache.
 Exemple   : $task -> setExclusive(1);
 Retour    : undef ou 1, l'excusivité du la tache.
 Arguments : 1, 1 pour rendre une tache excusive à un noeud.

=cut
sub setExclusive {
    my ($self, $arg) = @_;
    my $tmp = $self -> getExclusive();
    $self->{ EXCLUSIVE } = $arg;
    return $tmp;
}

=head2 setQos

 Nom       : setQos
 Fonction  : Permet de définir la QOS de la tache.
 Exemple   : $task -> setQos("long");
 Retour    : STRING, l'ancienne QOS.
 Arguments : STRING, le nom de la QOS.

=cut
sub setQos {
    my ($self, $arg) = @_;
    my $tmp = $self -> getQos();
    $self->{ QOS } = $arg;
    return $tmp;
}

=head2 setExtraParameters

 Nom       : setExtraParameters
 Fonction  : Permet de définir les parametres a passer directement à SLURM : -E "-slurmPara"
 Exemple   : $task -> setExtraParameters("-paraDeSlurm");
 Retour    : STRING, les anciens parametres.
 Arguments : STRING, les parametres à passer dirrectement à SLURM.

=cut
sub setExtraParameters {
    my ($self, $arg) = @_;
    my $tmp = $self -> getExtraParameters();
    $self->{ EXTRA_PARAMETERS } = $arg;
    return $tmp;
}

=head2 setStartTime

 Nom       : setStartTime
 Fonction  : Permet de définir une date de début pour la tache.
 Exemple   : $task -> setStartTime("01/01 12:00");
 Retour    : STRING, l'ancien temps de lancement.
 Arguments : STRING, le moment de lancement. "HH:MM" ou "MM/DD HH:MM"

=cut
sub setStartTime {
    my ($self, $arg) = @_;
    my $tmp = $self -> getStartTime();
    $self->{ START_TIME } = $arg;
    return $tmp;
}

=head2 setMailOpts

 Nom       : setProjId
 Fonction  : Permet de définir les options pour l'envoie d'un mail.
 Exemple   : $task -> setMailOpts("jdoe@foo.com:begin,end");
 Retour    : STRING, les anciens options.
 Arguments : STRING, les options pour l'envoie d'un mail.

=cut
sub setMailOpts {
    my ($self, $arg) = @_;
    my $tmp = $self -> getMailOpts();
    $self->{ MAIL_OPTS } = $arg;
    return $tmp;
}

##########
## GETEURS
########## 
=head2 getOptsTask

 Nom       : getOptsTask
 Fonction  : Enregistre les options de la tache Slurm
 Exemple   : $task -> getOptsTask();
 Retour    : STRING, les options 
 Arguments : NULL

=cut
sub getOptsTask {
    my $self = shift;
    return $self->{ OPTS_TASK };
}

=head2 getRam

 Nom       : getRam
 Fonction  : Renvoie la ram par core à réserver.
 Exemple   : $task->getRam();
 Retour    : Number, la ram à réserver par core.
 Arguments : Null.

=cut
sub getRam {
    my $self = shift;
    return $self->{ RAM };
}

=head2 getQueue

 Nom       : getQueue
 Fonction  : Renvoie le nom de la queue.
 Exemple   : $task -> getQueue();
 Retour    : STRING, le nom de la queue à utiliser.
 Arguments : Null.

=cut
sub getQueue {
    my $self = shift;
    return $self->{ QUEUE };
}

=head2 getProjId

 Nom       : getProjId
 Fonction  : Renvoie le nom du goupe.
 Exemple   : $task -> getProjId();
 Retour    : STRING, le nom du groupe.
 Arguments : Null.

=cut
sub getProjId {
    my $self = shift;
    return $self->{ GROUPE };
}

=head2 getTimeLimit

 Nom       : getTimeLimit
 Fonction  : Renvoie le temps max de la tache.
 Exemple   : $task -> getTimeLimit("120");
 Retour    : INT, le temps max de la tache.
 Arguments : RIEN

=cut
sub getTimeLimit {
    my $self = shift;
    return $self->{ TIME_LIMITE };
}

=head2 getExclusive

 Nom       : getExclusive
 Fonction  : Renvoie l'exclusivité de la tache.
 Exemple   : $task -> getExclusive();
 Retour    : INT, 1 si noeud excusif pour la tache, undef sinon.
 Arguments : RIEN

=cut
sub getExclusive {
    my $self = shift;
    return $self->{ EXCLUSIVE };
}

=head2 getQos

 Nom       : getQos
 Fonction  : Renvoie la QOS de la tache.
 Exemple   : $task -> getQos();
 Retour    : STRING, l'ancienne QOS.
 Arguments : RIEN.

=cut
sub getQos {
    my $self = shift;
    return $self->{ QOS };
}

=head2 getExtraParameters

 Nom       : getExtraParameters
 Fonction  : Renvoie les parametres a passer directement à SLURM : -E "-slurmPara"
 Exemple   : $task -> getExtraParameters();
 Retour    : STRING, les parametres pour SLURM.
 Arguments : RIEN.

=cut
sub getExtraParameters {
    my $self = shift;
    return $self->{ EXTRA_PARAMETERS };
}

=head2 getStartTime

 Nom       : getStartTime
 Fonction  : Renvoie la date de début prévu de la tache.
 Exemple   : $task -> getStartTime();
 Retour    : STRING, le temps de lancement prévu de la tache.
 Arguments : RIEN.

=cut
sub getStartTime {
    my $self = shift;
    return $self->{ START_TIME };
}

=head2 getMailOpts

 Nom       : getMailOpts
 Fonction  : Renvoie les options pour l'envoie d'un mail.
 Exemple   : $task -> getMailOpts("jdoe@foo.com:begin,end");
 Retour    : STRING, les options pour l'enovie du mail.
 Arguments : RIEN.

=cut
sub getMailOpts {
    my $self = shift;
    return $self->{ MAIL_OPTS };
}

=head2 cancel

 Nom       : cancel
 Fonction  : Annule la tache dans SLURM, avec  scancel.
 Exemple   : my $outCancel = $task -> cancel();
 Retour    : STRING, STDERR et STDOUT de la commande d'annulation.
 Arguments : Null.

=cut
sub cancel {
    my $self = shift;
    
    my $jobID = $self -> getID();
    my $output = `scancel $jobID 2>&1`;
    
    return $output;
}

=head2 taskFailed

 Nom       : taskFailed
 Fonction  : Retour 1 si le job est en echec, 0 sinon. 
 Exemple   : my $yesOrNo = $task -> taskFailed();
 Retour    : boolean, 1 le job est en echec, 0 sinon
 Arguments : Null.

=cut
sub taskFailed {
    my $self = shift;
    
    my $state = $self -> getState();
    if($state =~ m/FAILED/ or $state =~ m/NODE_FAIL/ or $state =~ m/PREEMPTED/ or $state =~ m/TIMEOUT/ or $state =~ m/CANCELLED/){
    	return 1;
    }elsif($_ -> getReturn() eq ""){
    	warn "Impossible de determiner le code retour du job ".$self -> getName()." (".$self -> getID().")\n";
    	return 0;
    }elsif($_ -> getReturn() ne "0:0"){
    	return 1;
    }else{
    	return 0;
    }
}

1;
