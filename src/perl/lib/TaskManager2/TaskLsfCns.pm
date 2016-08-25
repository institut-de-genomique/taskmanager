################################################################################
#
# * $Id: TaskLsfCns.pm,v 0.1 2014/04/2 akourlai Exp $
#
# * TaskLsfCns module for TaskManager2::TaskLsfCns.pm
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

   TaskManager2::TaskLsfCns.pm - Tache pour soumission à LSF du CNS.

=head1 SYNOPSIS

   Utiliser TaskManager2::ManagerFactory pour créer des taches.
    
=head1 DESCRIPTION

    TaskManager2::TaskLsfCns.pm - Tache pour soumission à LSF du CNS.

=head1 AUTHOR

N'hésitez pas à me contacter pour tout renseignement, ajout de fonctionalité ou signalement de bugs.

Artem Kourlaiev, akourlai@genoscope.cns.fr

=cut

package  TaskManager2::TaskLsfCns;

use  TaskManager2::Task;
our @ISA = qw( TaskManager2::Task);

use strict;
use overload '""'  =>  \&_display;

=head2 new

 Nom       : new
 Fonction  : Crée un objet de la class TaskLsfCns, class fille de Task. Pour des wrappers susceptibles d'être exécutés dans différents environnement (CNS, CNG, CCRT) utiliser le module TaskManager2::TaskFactory pour créer des taches.
 Exemple   : my $task = TaskManager2::TaskLsfCns -> new($taskName, $cmd, $opts);
 Retour    : TaskLsfCns. une référence sur un objet de la class TaskLsfCns.
 Arguments : STRINGS. $taskName, $cmd, $opts

=cut
sub new {
    my ($class, $name, $cmd, $opts) = @_;
    #la ligne suivante appelle le constructeur de Task
    my $self = $class -> SUPER::new($name, $cmd);
    bless( $self, $class);#lie la référence à la classe courante
    
    $self -> setOpts($opts);
    return $self;
}

=head2 _display

 Nom       : _display
 Fonction  : Permet d'afficher la tache formaté pour la soumission au LSF du CNS.
 Exemple   : print($task);
 Retour    : STRING, contenu du fichier run.sh pour une soumission à LSF dans l'environnement du CNS. Avec la commande bsub.
 Arguments : Null.

=cut
sub _display {
    my $self = shift;
 
    my $res = "#!/bin/bash\n";
    $res .= "#BSUB -J ".$self -> getName()."\n";
    $res .= "#BSUB -o ".$self -> getOutDir() . "/\%J.out\n";
    $res .= "#BSUB -e ".$self -> getOutDir() . "/\%J.err\n\n";  
    $res .= "set -e;\n";
    $res .= "set -o pipefail;\n"; 
    $res .= "set -u;\n"; 
    
    $res .= "\n".$self -> getCmd()."\n\n";
    
    return $res;
}

=head2 makeCmdToExecute

 Nom       : makeCmdToExecute
 Fonction  : Fabrique la ligne de comande pour une soumission à LSF avec bsub au CNS.
 Exemple   : $task -> makeCmdToExecute();
 Retour    : Null.
 Arguments : Null.

=cut
sub makeCmdToExecute {
    my $self = shift;
    
    my $scriptPath =  $self -> getFileRunPath();                                                         
    my $cmdToExecute = "unset BSUB_QUIET; unset BSUB_QUIET2; BSUB_STDOUT=y; BSUB_STDERR=y; bsub -I ".$self -> getOpts()." < $scriptPath ";
    $self -> setCmdToExecute($cmdToExecute);
    return $self -> getCmdToExecute();
}

=head2 cancel

 Nom       : cancel
 Fonction  : Annule la tache dans LSF.
 Exemple   : $task -> cancel($id);
 Retour    : STRING, STDERR et STDOUT de la commande d'annulation.
 Arguments : STRING. ID LSF de la tache.

=cut
sub cancel {
    my ($self, $arg) = @_;
    
    my $jobID = $arg;
    my $output = `bkill $jobID 2>&1`;
    
    return $output;
}

=head2 extractID

 Nom       : extractID
 Fonction  : Récupère le ID de la tache soumise à LSF.
 Exemple   : my $slurmId = $task -> extractID();
 Retour    : STRING, ID LSF de la tache.
 Arguments : STRING, le out après la soumission à LSF : "... is submitted ... 12345".

=cut
sub extractID{
    my ($self, $output) = @_;
    
    if($output =~ m/is submitted/){
        $self -> setSubmitTime();
        my @tab = split (" ", $output);
        my $jobID = $tab[1];
        $jobID = substr($jobID, 1, -1);
        $self -> setID($jobID);
        return 1;
    }else{
        return $output;
    }
}

=head2 setOpts

 Nom       : setOpts
 Fonction  : Associe les options à la tache LSF du CNS.
 Exemple   : $task -> setOpts("-q normal");
 Retour    : STRING, les anciens options.
 Arguments : STRING, les options pour la tache LSF.

=cut
sub setOpts {
    my ($self, $arg) = @_;
    if (! defined $arg){
		die "La méthode \$task -> setOpts() prend obligatoirement un argument."; 
	}
	my $oldOpts = $self -> getOpts();
    $self -> { OPTS } = $arg;
    return $oldOpts;
}

=head2 getOpts

 Nom       : getOpts
 Fonction  : Renvoie les options de la tache LSF.
 Exemple   : $task -> getOpts();
 Retour    : STRING, les options de la tache LSF.
 Arguments : Null.

=cut
sub getOpts {
    my $self = shift;
    return $self -> { OPTS };
}



1;

