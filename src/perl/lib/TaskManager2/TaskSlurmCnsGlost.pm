################################################################################
#
# * $Id: TaskSlurmCnsGlost.pm,v 0.1 2014/04/2 akourlai Exp $
#
# * TaskSlurmCnsGlost module for TaskManager2::TaskSlurmCnsGlost.pm
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

TaskManager2::TaskSlurmCnsGlost.pm - Tache GLOST pour soumission au SLURM du CNS sans la surcouche BRIDGE.

=head1 SYNOPSIS

    Utiliser TaskManager2::ManagerFactory pour cr�er des taches.
    
=head1 DESCRIPTION

    TaskManager2::TaskSlurmCnsGlost.pm - Tache GLOST pour soumission au SLURM du CNS sans la surcouche BRIDGE.

=head1 AUTHOR

N'h�sitez pas � me contacter pour tout renseignement, ajout de fonctionalit� ou signalement de bugs.

Artem Kourlaiev, akourlai@genoscope.cns.fr

=cut

package  TaskManager2::TaskSlurmCnsGlost;

use  TaskManager2::TaskSlurm;
our @ISA = qw( TaskManager2::TaskSlurm);

use strict;
use overload '""'  =>  \&_display;

=head2 new

 Nom       : new
 Fonction  : Cr�e un objet de la class TaskSlurmCns, class fille de TaskSlurm. Pour des wrappers susceptibles d'�tre ex�cut�s dans diff�rents environnement (CNS, CNG, CCRT) utiliser le module TaskManager2::TaskFactory pour cr�er des taches.
 Exemple   : my $task = TaskManager2::TaskSlurmCnsGlost -> new($taskName, \@cmds, $nbr_noeud, $nbr_coeur_par_noeud, $queue, $groupe, $ramParNoeud);
 Retour    : une r�f�rence sur un objet de la class TaskSlurm.
 Arguments : STRINGS. $nomDelaTache, $cmd, $nbr_noeud, $nbr_coeur_par_noeud, $queue, $groupe, $ramParNoeud.

=cut
sub new {
    my $class = shift;
    my ($name, $refTabCmds, $nbrTask, $nbrCorePerTask, $mem_limit, $time_limit, $exculisive, $queue, $projid, $qos, $extra_parameters, $starttime, $mailopts, $taskPath) = @_;
        
    #la ligne suivante appelle le constructeur de TaskSlurm
    my $self = $class -> SUPER::new($name, "mpirun glost_launch ".$taskPath."/".$name."/cmds.glost", $nbrTask, $nbrCorePerTask, $mem_limit, $time_limit, $exculisive, $queue, $projid, $qos, $extra_parameters, $starttime, $mailopts);
    bless( $self, $class); #lie la r�f�rence � la classe courante
    
    $self -> setCmds($refTabCmds);
    $self -> setOutDir($taskPath);
    return $self;
}

=head2 _display

 Nom       : _display
 Fonction  : Permet d'afficher la tache format� pour la soumission au Batch Manager SLURM du CNS (run.sh).
 Exemple   : print($task);
 Retour    : STRING, contenu du fichier run.sh pour une soumission � SLURM dans l'environnement du CNS. Avec la commande sbatch.
 Arguments : Null.

=cut
sub _display {
    my $self = shift;
 
    my $res = "#!/bin/bash\n";
    $res .= "#SBATCH -J ".$self -> getName()."\n";
    $res .= "#SBATCH -A ".$self -> getProjId()."\n";
    $res .= "#SBATCH -p ".$self -> getQueue()."\n";
    $res .= "#SBATCH -n ".$self -> getNbrTask()."\n";
    $res .= "#SBATCH -c ".$self -> getNbrCorePerTask()."\n";
    if($self -> getRam()){
        $res .= "#SBATCH --mem-per-cpu=".$self -> getRam()." #Mo\n";
    }
    if($self -> getTimeLimit() and $self -> getTimeLimit() != 0){
        $res .= "#SBATCH -t ".int($self -> getTimeLimit()/60)." # mins\n";
    }
    if($self -> getExclusive()){
        $res .= "#SBATCH --exclusive\n";
    }
    if($self -> getQos()){
        $res .= "#SBATCH --qos=".$self -> getQos()."\n";
    }
    if($self -> getStartTime()){
        $res .= "#SBATCH --begin=".$self -> getStartTime()."\n";
    }
    if($self -> getMailOpts()){
        $res .= "#SBATCH --mail-user=".$self -> getMailOpts()."\n";
    }
    $res .= "#SBATCH -o ". $self -> getOutDir() . "/\%j.out\n";
    $res .= "#SBATCH -e ". $self -> getOutDir() . "/\%j.err\n\n";
    foreach (@{$self -> getModules()}) {
        $res .= $_."\n";
    }     
    $res .= "module load glost\n";
    $res .= "set -e;\n";
    $res .= "set -o pipefail;\n"; 
    $res .= "set -u;\n"; 
    
    $res .= "\n".$self -> getCmd()."\n\n";
    
    return $res;
}

=head2 makeCmdToExecute

 Nom       : makeCmdToExecute
 Fonction  : Fabrique la ligne de comande pour une soumission � SLURM avec sbatch. Rajoute les d�pendances et redirige STDERR vers le STDOUT.
 Exemple   : $task -> makeCmdToExecute();
 Retour    : Null.
 Arguments : Null.

=cut
sub makeCmdToExecute {
    my $self = shift;
    
    $self -> _makeGlostCmdFile($self -> getCmds()); # Cette fonction cr�e le fichier de commandes n�cessaire au GLOST.
    
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
    }
    my $cmdToExecute;
    
    if(defined $self -> getExtraParameters()){
        $cmdToExecute = "sbatch ".$self -> getExtraParameters()." $scriptDep $scriptPath 2>&1";
    }else{
        $cmdToExecute = "sbatch $scriptDep $scriptPath 2>&1";
    }
   
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
 Fonction  : R�cup�re le ID de la tache soumise � SLURM.
 Exemple   : my $slurmId = $task -> extractID();
 Retour    : STRING, ID SLURM de la tache.
 Arguments : STRING, le out apr�s la soumission � SLURM : "Submitted batch job 12345" ou "Submitted Batch Session 12345".

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


=head2 _makeGlostCmdFile

 Nom       : _makeGlostCmdFile
 Fonction  : Cr�e le fichier avec toutes les commande pour lancer des taches parellele avec GLOST, cr�ation du fichier regroupant toutes les commandes � ex�cuter par GLOST. Les taches regroup�es sous GLOST sont remplac�es par une tache ma�tre GLOST.
 Exemple   : $self -> _makeGlostCmdFile(\@tasksToSubmit);
 Retour    : STRING, path du fichier des commandes.
 Arguments : REF, r�f�rence vers un tableau de taches � �x�cuter en parellele avec GLOST.

=cut
sub _makeGlostCmdFile{
     my ($self, $refTabCmds) = @_;
     my @allGlostCmds = @{$refTabCmds}; #transformation de la reference en en tableau de commandes.
     my $glostCmdPath = $self -> getOutDir()."/cmds.glost";
     
     open(FILE,">".$glostCmdPath) or die("Probl�me avec le script des commandes de GLOST ($glostCmdPath): open: $!");
     
     for(@allGlostCmds){
         my $cmd = $_;
         print FILE $cmd."\n";
     }
     close(FILE);
     $self -> setCmdsFilePath($glostCmdPath);
     
     return "mpirun glost_launch ".$glostCmdPath;
}


=head2 setCmdsFilePath

 Nom       : setCmdsFilePath
 Fonction  : Definit le path du fichier des commandes de GLOST.
 Exemple   : $task -> setCmdsFilePath($path);
 Retour    : REF, l'ancien path.
 Arguments : REF, le path du fichier des commandes de GLOST.

=cut
sub setCmdsFilePath {
    my ($self, $arg) = @_;
    if (! defined $arg){
 		die "La m�thode \$task -> setCmdsFilePath() prend obligatoirement un argument."; 
    }
    my $tmp = $self -> getCmdsFilePath();
    $self->{ PATH_CMDS } = $arg;
    return $tmp;
}

=head2 getCmdsFilePath

 Nom       : getCmdsFilePath.
 Fonction  : Renvoie le path du fichier des commandes de GLOST.
 Exemple   : $task -> getCmdsFilePath();
 Retour    : (STRING), le path du fichier des commandes de GLOST.
 Arguments : Null.

=cut
sub getCmdsFilePath {
    my $self = shift;
    if(!defined $self->{ PATH_CMDS }){
        $self->{ PATH_CMDS } = "";
    }
    return $self->{ PATH_CMDS };
}


=head2 setCmds

 Nom       : setCmds
 Fonction  : Stock la reference du tableau des taches.
 Exemple   : $task -> setCmds(\@cmds);
 Retour    : REF, l'ancien ref.
 Arguments : REF, la reference du tableau des taches.

=cut
sub setCmds {
    my ($self, $arg) = @_;
    if (! defined $arg){
 		die "La m�thode \$task -> setCmds() prend obligatoirement un argument."; 
    }
    my $tmp = $self -> getCmds();
    $self->{ CMDS } = $arg;
    return $tmp;
}

=head2 getCmds

 Nom       : getCmds.
 Fonction  : Renvoie la reference du tableau des taches.
 Exemple   : $task -> getCmds();
 Retour    : (STRING), la reference du tableau des taches.
 Arguments : Null.

=cut
sub getCmds {
    my $self = shift;
    if(!defined $self->{CMDS }){
        $self->{ CMDS } = "";
    }
    return $self->{ CMDS };
}



1;

