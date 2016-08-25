################################################################################
#
# * $Id: TaskLocal.pm,v 0.1 2014/04/2 akourlai Exp $
#
# * TaskLocal module for TaskManager2::TaskLocal.pm
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

   TaskManager2::TaskLocal.pm - Tache pour soumission en Local.

=head1 SYNOPSIS

   Utiliser TaskManager2::ManagerFactory pour créer des taches.
    
=head1 DESCRIPTION

    TaskManager2::TaskLocal.pm - Tache pour soumission en Local.

=head1 AUTHOR

N'hésitez pas à me contacter pour tout renseignement, ajout de fonctionalité ou signalement de bugs.

Artem Kourlaiev, akourlai@genoscope.cns.fr

=cut

package  TaskManager2::TaskLocal;

use  TaskManager2::Task;
our @ISA = qw( TaskManager2::Task);

use strict;
use overload '""'  =>  \&_display;
use POSIX qw( :sys_wait_h );

=head2 new

 Nom       : new
 Fonction  : Crée un objet de la class TaskLocal, class fille de Task. Utilisez le module TaskManager2::TaskFactory pour créer des taches.
 Exemple   : $task = TaskManager2::TaskLocal -> new($taskName, $cmd);
 Retour    : une référence sur un objet de la class TaskLocal.
 Arguments : STRINGS. $taskName, $cmd.

=cut
sub new {
    my $class = shift;
    #la ligne suivante appelle le constructeur de Task
    my $self = $class -> SUPER::new(@_);
    bless( $self, $class);#lie la référence à la classe courante
    return $self;
}

=head2 _display

 Nom       : _display
 Fonction  : Permet d'afficher la tache formaté pour la soumission en Local.
 Exemple   : print($task);
 Retour    : STRING, contenu du fichier run.sh pour une soumission en Local.
 Arguments : Null.

=cut
sub _display {
    my $self = shift;
    my $res = "";
    
    foreach (@{$self -> getModules()}) {
        $res .= $_."\n";
    }   
    
    $res .= "set -e;\nset -o pipefail;\nset -u;\n\n";
    
    $res .= $self -> getCmd()."\n";
    return $res;
}

=head2 makeCmdToExecute

 Nom       : makeCmdToExecute
 Fonction  : Fabrique la ligne de comande pour une soumission en Local.
 Exemple   : $task -> makeCmdToExecute();
 Retour    : Null.
 Arguments : Null.

=cut
sub makeCmdToExecute {
    my $self = shift;
    
    my $scriptPath =  $self -> getFileRunPath();                                                         
    my $cmdToExecute = "$scriptPath 2>".$self->getOutDir()."/".$self -> getName().".err 1>".$self->getOutDir()."/".$self -> getName().".out\n";;
    $self -> setCmdToExecute($cmdToExecute);
    return $self -> getCmdToExecute();
}

1;

