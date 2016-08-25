################################################################################
#
# * $Id: TaskSlurmCcrt.pm,v 0.1 2014/04/2 akourlai Exp $
#
# * TaskSlurmCcrt module for TaskManager2::TaskSlurmCcrt.pm
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

TaskManager2::TaskSlurmCcrt.pm - Tache pour soumission au SLURM du Ccrt.

=head1 SYNOPSIS

    Utiliser TaskManager2::ManagerFactory pour créer des taches.
    
=head1 DESCRIPTION

    TaskManager2::TaskSlurmCcrt.pm - Tache pour soumission au SLURM du Ccrt.

=head1 AUTHOR

N'hésitez pas à me contacter pour tout renseignement, ajout de fonctionalité ou signalement de bugs.

Artem Kourlaiev, akourlai@genoscope.cns.fr

=cut

package  TaskManager2::TaskSlurmCcrt;

use  TaskManager2::TaskSlurm;
our @ISA = qw( TaskManager2::TaskSlurm);

use strict;
use overload '""'  =>  \&_display;

=head1 NAME

TaskManager2::BatchManager::TaskSlurmCNS.pm - Class ABSTRAITE qui gère les fonctionalités communes des taches pour la soumission à un Batch Manager. 

=head1 SYNOPSIS

    NE PAS UTILISER DIRECTEMENT, PASSER PAR TaskManager2::BatchManager::TaskFactory.pm pour créer des taches !!!
    
=head1 DESCRIPTION

    NE PAS UTILISER DIRECTEMENT, PASSER PAR TaskManager2::BatchManager::TaskFactory.pm pour créer des taches !!!

=head1 AUTHOR

N'hésitez pas à me contacter pour tout renseignement, ajout de fonctionalité ou signalement de bugs.

Artem Kourlaiev, akourlai@genoscope.cns.fr

=cut

package  TaskManager2::TaskSlurmCcrt;

use  TaskManager2::TaskSlurm;
@ISA = qw(TaskManager2::TaskSlurm);

use strict;
use overload '""'  =>  \&_display;

=head2 new

 Nom       : new
 Fonction  : Crée un objet de la class TaskSlurmCcrt, class fille de TaskSlurm. Pour des wrappers susceptibles d'être exécutés dans différents environnement (CNS, CNG, CCRT) utiliser le module TaskManager2::TaskFactory pour créer des taches.
 Exemple   : my $task = TaskManager2::TaskSlurmCns -> new($taskName, $cmd, $nbr_noeud, $nbr_coeur_par_noeud, $queue, $groupe, $ramParNoeud);
 Retour    : une référence sur un objet de la class TaskSlurm.
 Arguments : STRINGS. $nomDelaTache, $cmd, $nbr_noeud, $nbr_coeur_par_noeud, $queue, $groupe, $ramParNoeud.

=cut
sub new {
    my $class = shift;
    #la ligne suivante appelle le constructeur de TaskSlurm
    my $self = $class -> SUPER::new(@_);
    bless( $self, $class);#lie la référence à la classe courante
    return $self;
}

=head2 _display

 Nom       : _display
 Fonction  : Permet d'afficher la tache formaté pour la soumission au Batch Manager SLURM du CCRT (run.sh).
 Exemple   : print($task);
 Retour    : STRING, contenu du fichier run.sh pour une soumission à SLURM dans l'environnement du CCRT. Avec la commande sbatch.
 Arguments : Null.

=cut
sub _display {
    my $self = shift;
 
    my $res = "#!/bin/bash\n";
    $res .= "#MSUB -r ".$self -> getName()."\n";
    $res .= "#MSUB -p ".$self -> getProjId()."\n";
    $res .= "#MSUB -q ".$self -> getQueue()."\n";
    $res .= "#MSUB -n ".$self -> getNbrTask()."\n";
    $res .= "#MSUB -c ".$self -> getNbrCorePerTask()."\n";
    if($self -> getRam()){
        $res .= "#MSUB -M ".$self -> getRam()."\n"; # memory per allocated core
    }
    if($self -> getTimeLimit() and $self -> getTimeLimit() != 0){
        $res .= "#MSUB -T ".$self -> getTimeLimit()."# secs\n";
    }
    if($self -> getExclusive()){
        $res .= "#MSUB -x\n";
    }
    if($self -> getQos()){
        $res .= "#MSUB -Q ".$self -> getQos()."\n";
    }
    if(defined $self -> getExtraParameters()){
        $res .= "#MSUB -E \"".$self -> getExtraParameters()."\"\n";
    }
    if($self -> getStartTime()){
        $res .= "#MSUB -S ".$self -> getStartTime()."\n";
    }
    if($self -> getMailOpts()){
        $res .= "#MSUB -\@ ".$self -> getMailOpts()."\n";
    }
    $res .= "#MSUB -o ". $self -> getOutDir() . "/\%j.out\n";
    $res .= "#MSUB -e ". $self -> getOutDir() . "/\%j.err\n\n";
    foreach (@{$self -> getModules()}) {
        $res .= $_."\n";
    }   
    
    $res .= "set -e;\n";
    $res .= "set -o pipefail;\n"; 
    $res .= "set -u;\n"; 
    
    $res .= "\nccc_mprun ".$self -> getCmd()."\n";
    
    return $res;
}

=head2 makeCmdToExecute

 Nom       : makeCmdToExecute
 Fonction  : Fabrique la ligne de comande pour une soumission à SLURM avec sbatch. Rajoute les dépendances et redirige STDERR vers le STDOUT.
 Exemple   : $task -> makeCmdToExecute();
 Retour    : Null.
 Arguments : Null.

=cut
sub makeCmdToExecute {
    my $self = shift;
    
    my $submit = 0;
    my $scriptDep = "";
    my @taskDep = @{$self -> getDep()};
    
    my $scriptPath =  $self -> getFileRunPath();
                                                                                       
    if (@taskDep != 0){
        $scriptDep = "-dafterok";
        foreach(@taskDep){
            my $task2 = $_;
            $scriptDep .= ":".$task2 -> getID();
        }
        $scriptDep = "-E \"".$scriptDep."\"";
    }
    my $cmdToExecute = "ccc_msub $scriptDep $scriptPath 2>&1";
    
    $self -> setCmdToExecute($cmdToExecute);
    return $self -> getCmdToExecute();
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

=head2 extractID

 Nom       : extractID
 Fonction  : Récupère le ID de la tache soumise à SLURM.
 Exemple   : my $slurmId = $task -> extractID();
 Retour    : STRING, ID SLURM de la tache.
 Arguments : STRING, le out après la soumission à SLURM : "Submitted batch job 12345" ou "Submitted Batch Session 12345".

=cut
sub extractID{
    my ($self, $output) = @_;
    
    if($output =~ m/Submitted batch job/ || $output =~ m/Submitted Batch Session/){ # messages de soumission reussi de SLURM (sbatch) et BRIDGE (ccc_msub)
        my @tab = split (" ", $output);
        my $jobID = $tab[3];
        
        $self -> setID($jobID);
        return 1;
    }else{
        return $output;
    }
}

1;