################################################################################
#
# * $Id: TaskFactory.pm,v 0.1 2014/04/2 akourlai Exp $
#
# * TaskFactory module for TaskManager2::TaskFactory.pm
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

TaskManager2::TaskFactory.pm - C'est factory qui permet de cr�er des taches sp�cifiques � l'environnement de soumission. Ainsi des pipelines utilisant cette factory peuvent etre lanc� 
sans ou avec Batch Manager et dans diff�rents environnement (CNS, CNG, CCRT, ...). Et suivant les options -BMCMD et -BMOPT du wrapper.

=head1 SYNOPSIS

     Voir TaskManager2::ManagerFactory.
    
=head1 DESCRIPTION

Factory qui permet de cr�er des taches sp�cifiques � l'environnement de soumission. Ainsi des pipelines utilisant cette factory peuvent etre lanc� 
sans ou avec diff�rents BatchManager et dans diff�rent environnement (CNS, CNG, CCRT, ...). Et suivant les options -BMCMD et -BMOPT du wrapper.
    
=head1 AUTHOR

N'h�sitez pas � me contacter pour tout renseignement, ajout de fonctionalit� ou signalement de bugs.

Artem Kourlaiev, akourlai@genoscope.cns.fr

=cut
package  TaskManager2::TaskFactory;
require Exporter;

use vars qw(@EXPORT @ISA); 
@ISA = qw(Exporter);
@EXPORT = qw(createTask);

# Read config file
BEGIN { 
    
}
# Use correct lib function to host

use strict;
use Net::Domain qw(hostdomain hostname);
use Carp;
use TaskManager2::TaskSlurmCns;
use TaskManager2::TaskSlurmCnsGlost;
use TaskManager2::TaskSlurmCng;
use TaskManager2::TaskSlurmCcrt;
use TaskManager2::TaskSlurmCcrtGlost;
use TaskManager2::TaskLsfCns;
use TaskManager2::TaskLocal;
use TaskManager2::TaskSsh;
use TaskManager2::TaskGsissh;


our $BM_CMD = $TaskManager2::ManagerFactory::BM_CMD;
our %SLURM_DEFAULT_OPTS = %TaskManager2::ManagerFactory::SLURM_DEFAULT_OPTS;
our $LSF_DEFAULT_OPTS = $TaskManager2::ManagerFactory::LSF_DEFAULT_OPTS;


=head2 new

 Nom       : new
 Fonction  : Cr�e l'objet TaskManager2::TaskFactory.
 Exemple   : TaskManager2::TaskFactory -> new();
 Retour    : une r�f�rence sur un objet de la class TaskFactory.
 Arguments : Null.

=cut
sub new {
    my $class = shift;
    $class = ref($class) || $class;
    my $self = {};
    bless($self, $class);
    return $self;
}

=head2 createTask

 Nom       : createTask
 Fonction  : Cr�e un objet Task adapt� � l'environnement d'execution du script. Si certains options du BatchManager sont absents, les valeurs par d�fault contenues dans ManagerFactory sont utilis�es pour la cr�ation des taches.
 Exemple   : TaskManager2::TaskFactory -> createTask($taskName, $cmd, $batchType, $batchOpts);
           : my @batch = ("bsub", "msub");
           : my %batchOpts = ("bsub" => "-q big", "msub" => "-q xlarge");
           : TaskManager2::TaskFactory -> createTask($taskName, $cmd, \@batchType, \%batchOpts);
 Retour    : une r�f�rence sur un objet de la class Task.
 Arguments : STRINGS sauf $batchType qui peut �tre une ref sur un tableau de String et $batchOpts une ref sur une table de hash.
 

=cut

sub createTask{
    my ($self, $taskName, $cmd, $batch, $jobOpts, $taskPath) = @_;
    my $task;
        
    my %slurmJobOptsToSubmit = %SLURM_DEFAULT_OPTS;
    my $lsfJobOptsToSubmit = $LSF_DEFAULT_OPTS;
    
    if($BM_CMD eq "msub"){
        ### Surcharge des options BMCMD du pipeline, au niveau des createJob ###
       	if(defined $batch && ($batch eq $BM_CMD || ref($batch) eq "ARRAY" && scalar(grep(/$BM_CMD/, @{$batch})))){
            $batch = $BM_CMD;
            
            if(defined $jobOpts){
                if(ref($jobOpts) eq "HASH"){
                    my %jobsOptsHash = %{$jobOpts};
                    if(exists $jobsOptsHash{$batch}){
                        $jobOpts = $jobsOptsHash{$batch};
                    }
                }
                %slurmJobOptsToSubmit = %{TaskManager2::ManagerFactory -> _setSlurmOpts(\%slurmJobOptsToSubmit, $jobOpts)}; #Les options du pipeline sont surchag�es pour ce job.
                print $self -> toStringOverloadOptions($taskName, $cmd, $batch, $self -> toStringOpts(\%slurmJobOptsToSubmit));
            } 
       	}
       	
       	if(defined $batch && ($batch eq "msub" || $batch eq "")){
       	    $task = $self -> _createSlurmTask($taskName, $cmd, \%slurmJobOptsToSubmit);
       	    $task -> setOptsTask(\%slurmJobOptsToSubmit);
       	}elsif(defined $batch && $batch eq "gsissh"){
       	    $task = TaskManager2::TaskGsissh -> new($taskName, $cmd, $jobOpts);
       	}elsif(defined $batch && $batch eq "ssh"){
       	    $task = TaskManager2::TaskSsh -> new($taskName, $cmd, $jobOpts);
       	}elsif(defined $batch && $batch eq "-"){
       	    $task = TaskManager2::TaskLocal -> new($taskName, $cmd);
       	}else{
       		my $errorMessage = "";
       		$errorMessage .= "[".ref($self)."] Verifiez si le Batch Manager du createJob est compatible avec le BatchManager du pipeline !\n";
  	        $errorMessage .= "[".ref($self)."] Job Name : $taskName\n";
  	        $errorMessage .= "[".ref($self)."] Command : $cmd\n";
  	        $errorMessage .= "[".ref($self)."] CreateJob type : $batch\n";
  	        confess ($errorMessage);
       	}
    }elsif($BM_CMD eq "glost"){
        ### Surcharge des options du pipeline, au niveau des createJob ###
       	if(defined $batch){
       	    if(ref($batch) eq "ARRAY" && scalar(grep(/msub/, @{$batch})) || $batch eq "msub" || $batch eq "glost"){
       	        if(ref($batch) eq "ARRAY" && scalar(grep(/msub/, @{$batch}))){
       	            $batch = "msub";
       	        }
       	        
       	        if(defined $jobOpts){
                    if(ref($jobOpts) eq "HASH"){
                        my %jobsOptsHash = %{$jobOpts};
                        if(exists $jobsOptsHash{$batch}){
                            $jobOpts = $jobsOptsHash{$batch};
                        }
                    }
                    %slurmJobOptsToSubmit = %{TaskManager2::ManagerFactory -> _setSlurmOpts(\%slurmJobOptsToSubmit, $jobOpts)}; #Les options du pipeline sont surchag�es pour ce job.
                    print $self -> toStringOverloadOptions($taskName, $cmd, $batch, $self -> toStringOpts(\%slurmJobOptsToSubmit));
                }
       	    }
       	}
       	if(defined $batch && $batch eq "glost"){  
       	    $task = $self -> _createSlurmTaskGlost($taskName, $cmd, \%slurmJobOptsToSubmit, $taskPath);
       	    $task -> setOptsTask(\%slurmJobOptsToSubmit);
       	}elsif(defined $batch && ($batch eq "msub" || $batch eq "")){
       	    $task = $self -> _createSlurmTask($taskName, $cmd, \%slurmJobOptsToSubmit);
       	    $task -> setOptsTask(\%slurmJobOptsToSubmit);
       	}elsif(defined $batch && $batch eq "gsissh"){
       	    $task = TaskManager2::TaskGsissh -> new($taskName, $cmd, $jobOpts);
       	}elsif(defined $batch && $batch eq "ssh"){
       	    $task = TaskManager2::TaskSsh -> new($taskName, $cmd, $jobOpts);
       	}elsif(defined $batch && $batch eq "-"){
       	    $task = TaskManager2::TaskLocal -> new($taskName, $cmd);
       	}else{
       	    my $errorMessage = "";
       		$errorMessage .= "[".ref($self)."] Verifiez si le Batch Manager du createJob est compatible avec le BatchManager du pipeline !\n";
  	        $errorMessage .= "[".ref($self)."] Job Name : $taskName\n";
  	        $errorMessage .= "[".ref($self)."] Command : $cmd\n";
  	        $errorMessage .= "[".ref($self)."] CreateJob type : $batch\n";
  	        confess ($errorMessage);
       	}
    }elsif($BM_CMD eq "bsub"){
        ### Surcharge des options du pipeline, au niveau des createJob ###
       	if(defined $batch && ($batch eq $BM_CMD || ref($batch) eq "ARRAY" && scalar(grep(/$BM_CMD/, @{$batch})))){
            $batch = $BM_CMD;
            if(defined $jobOpts){
                if(ref($jobOpts) eq "HASH"){
                    my %jobsOptsHash = %{$jobOpts};
                    if(exists $jobsOptsHash{$batch}){
                        $jobOpts = $jobsOptsHash{$batch};
                    }
                }
                $lsfJobOptsToSubmit = $jobOpts; # Toutes les options du pipeline sont ecras�es pour ce job.
                print $self -> toStringOverloadOptions($taskName, $cmd, $batch, $lsfJobOptsToSubmit);
            } 
       	}
        if(defined $batch && ($batch eq "bsub" || $batch eq "")){
       	    $task = $self -> _createLsfTask($taskName, $cmd, $lsfJobOptsToSubmit);
       	}elsif(defined $batch && $batch eq "gsissh"){
       	    $task = TaskManager2::TaskGsissh -> new($taskName, $cmd, $jobOpts);
       	}elsif(defined $batch && $batch eq "ssh"){
       	    $task = TaskManager2::TaskSsh -> new($taskName, $cmd, $jobOpts);
       	}elsif(defined $batch && $batch eq "-"){
       	    $task = TaskManager2::TaskLocal -> new($taskName, $cmd);
       	}else{
       	    my $errorMessage = "";
       		$errorMessage .= "[".ref($self)."] Verifiez si le Batch Manager du createJob est compatible avec le BatchManager du pipeline !\n";
  	        $errorMessage .= "[".ref($self)."] Job Name : $taskName\n";
  	        $errorMessage .= "[".ref($self)."] Command : $cmd\n";
  	        $errorMessage .= "[".ref($self)."] CreateJob type : $batch\n";
  	        confess ($errorMessage);
       	}
        
    }elsif($BM_CMD eq ""){
       	if(defined $batch && $batch eq "gsissh"){
       	    $task = TaskManager2::TaskGsissh -> new($taskName, $cmd, $jobOpts);
       	}elsif(defined $batch && $batch eq "ssh"){
       	    $task = TaskManager2::TaskSsh -> new($taskName, $cmd, $jobOpts);
       	}else{
       	    $task = TaskManager2::TaskLocal -> new($taskName, $cmd);
       	}
    }elsif($BM_CMD eq "-"){
        $task = TaskManager2::TaskLocal -> new($taskName, $cmd);
    }else{
        confess "[".ref($self)."] BatchManager non valide : \n -BMCMD = $BM_CMD\n";
    }
    return $task;
}

=head2 toStringOverloadOptions

 Nom       : toStringOverloadOptions
 Fonction  : G�nere un warning.
 Exemple   : $manager -> toStringOverloadOptions($task, $opts);
 Retour    : STRING
 Arguments : ref Task, STRING. Une r�f�rence vers l'objet Task et un String pour les options.

=cut
sub toStringOverloadOptions{
    my ($self, $taskName, $cmd, $batch, $opts) = @_;
    my $str = "";
    
    $str .=  "***************************************************************************************************\n";
    $str .=  "- WARNING - ".$taskName." - WARNING -\n";
    $str .=  "***************************************************************************************************\n";
    $str .=  "[".ref($self)."] Tache \"".$batch."\" pr�sente dans le pipeline.\n";
    $str .=  "[".ref($self)."] Command : ".$cmd."\n";
    $str .=  "[".ref($self)."] Options : \n".$opts;
    $str .=  "***************************************************************************************************\n";
    return "";
}

=head2 toStringOpts

 Nom       : toStringOpts
 Fonction  : Convertie en une chaine de caract�res les options du BatchManager, founi sous forme de hash.
 Exemple   : $manager -> toStringOpts($opts);
 Retour    : STRING
 Arguments : ref HASH. Une r�f�rence vers une hash d'options.

=cut
sub toStringOpts{
    my ($self, $opts) = @_;
    my %opts = %{$opts};
    my $str = "";
    $str .= "----------------------\n";
    $str .= "--- Job Options ---\n";
    $str .= "----------------------\n";
    
    foreach my $key (keys(%opts)){
        if(defined $opts{$key} && ref($opts{$key}) ne "ARRAY"){
            my $opt = $key." = ".$opts{$key}."\n";
            $str .= $opt;
        }
    }
    return $str;
}

=head2 _createSlurmTask

 Nom       : _createSlurmTask
 Fonction  : Cr�e une tache SLURM sp�cifique au domaine sur lequel est �x�cut� le wrapper.
 Exemple   : my $task = $manager -> _createSlurmTask($taskName, $cmd, $jobOptsToSubmit);
 Retour    : TASK
 Arguments : STRING STRING HASH. $taskName, $cmd, $jobOptsToSubmit

=cut
sub _createSlurmTask{
    my ($self, $taskName, $cmd, $jobOptsToSubmit) = @_;
    my $task;
    my %jobOptsToSubmit = %{$jobOptsToSubmit};
    
    if(hostdomain() =~ m/.*\.cns\.fr/){
        $task = TaskManager2::TaskSlurmCns -> new($taskName, $cmd, $jobOptsToSubmit{nbrTask}, $jobOptsToSubmit{nbrCorePerTask}, $jobOptsToSubmit{mem_limit}, $jobOptsToSubmit{time_limit}, $jobOptsToSubmit{exclusive}, $jobOptsToSubmit{queue}, $jobOptsToSubmit{projid}, $jobOptsToSubmit{qos}, $jobOptsToSubmit{extra_parameters}, $jobOptsToSubmit{starttime}, $jobOptsToSubmit{mailopts});
    }elsif(hostdomain() =~ m/.*cng\.fr/){
        $task = TaskManager2::TaskSlurmCng -> new($taskName, $cmd, $jobOptsToSubmit{nbrTask}, $jobOptsToSubmit{nbrCorePerTask}, $jobOptsToSubmit{mem_limit}, $jobOptsToSubmit{time_limit}, $jobOptsToSubmit{exclusive}, $jobOptsToSubmit{queue}, $jobOptsToSubmit{projid}, $jobOptsToSubmit{qos}, $jobOptsToSubmit{extra_parameters}, $jobOptsToSubmit{starttime}, $jobOptsToSubmit{mailopts});
    }elsif(hostdomain() =~ m/.*\.cea\.fr/){
        $task = TaskManager2::TaskSlurmCcrt -> new($taskName, $cmd, $jobOptsToSubmit{nbrTask}, $jobOptsToSubmit{nbrCorePerTask}, $jobOptsToSubmit{mem_limit}, $jobOptsToSubmit{time_limit}, $jobOptsToSubmit{exclusive}, $jobOptsToSubmit{queue}, $jobOptsToSubmit{projid}, $jobOptsToSubmit{qos}, $jobOptsToSubmit{extra_parameters}, $jobOptsToSubmit{starttime}, $jobOptsToSubmit{mailopts});
    }else{
        die "[".ref($self)."] TaskManager n'est pas configur� pour ce domaine avec ce type de BatchManager!\n[".ref($self)."] Host domaine : ".hostdomain()."\n[".ref($self)."] Host name : ".hostname()."\n[".ref($self)."] BatchManager : $BM_CMD\n";
    }
    return $task;
}

=head2 _createSlurmTaskGlost

 Nom       : _createSlurmTaskGlost
 Fonction  : Cr�e une tache SLURM GLOST sp�cifique au domaine sur lequel est �x�cut� le wrapper.
 Exemple   : my $task = $manager -> _createSlurmTaskGlost($taskName, $cmd, $jobOptsToSubmit, $taskPath);
 Retour    : TASK
 Arguments : STRING STRING HASH STRING. $taskName, $cmd, $jobOptsToSubmit, $taskPath (afin de cr�er le fichiers des commande dans le m�me r�pertoire).

=cut
sub _createSlurmTaskGlost{
    my ($self, $taskName, $cmd, $jobOptsToSubmit, $taskPath) = @_;
    my $task;
    my %jobOptsToSubmit = %{$jobOptsToSubmit};
    
    if(hostdomain() =~ m/.*\.cns\.fr/){
        $task = TaskManager2::TaskSlurmCnsGlost -> new($taskName, $cmd, $jobOptsToSubmit{nbrTask}, $jobOptsToSubmit{nbrCorePerTask}, $jobOptsToSubmit{mem_limit}, $jobOptsToSubmit{time_limit}, $jobOptsToSubmit{exclusive}, $jobOptsToSubmit{queue}, $jobOptsToSubmit{projid}, $jobOptsToSubmit{qos}, $jobOptsToSubmit{extra_parameters}, $jobOptsToSubmit{starttime}, $jobOptsToSubmit{mailopts}, $taskPath);
    }elsif(hostdomain() =~ m/.*cng\.fr/){
        $task = TaskManager2::TaskSlurmCnsGlost -> new($taskName, $cmd, $jobOptsToSubmit{nbrTask}, $jobOptsToSubmit{nbrCorePerTask}, $jobOptsToSubmit{mem_limit}, $jobOptsToSubmit{time_limit}, $jobOptsToSubmit{exclusive}, $jobOptsToSubmit{queue}, $jobOptsToSubmit{projid}, $jobOptsToSubmit{qos}, $jobOptsToSubmit{extra_parameters}, $jobOptsToSubmit{starttime}, $jobOptsToSubmit{mailopts}, $taskPath);
    }elsif(hostdomain() =~ m/.*\.cea\.fr/){
        $task = TaskManager2::TaskSlurmCcrtGlost -> new($taskName, $cmd, $jobOptsToSubmit{nbrTask}, $jobOptsToSubmit{nbrCorePerTask}, $jobOptsToSubmit{mem_limit}, $jobOptsToSubmit{time_limit}, $jobOptsToSubmit{exclusive}, $jobOptsToSubmit{queue}, $jobOptsToSubmit{projid}, $jobOptsToSubmit{qos}, $jobOptsToSubmit{extra_parameters}, $jobOptsToSubmit{starttime}, $jobOptsToSubmit{mailopts}, $taskPath);
    }else{
        die "[".ref($self)."] TaskManager n'est pas configur� pour ce domaine avec ce type de BatchManager!\n[".ref($self)."] Host domaine : ".hostdomain()."\n[".ref($self)."] Host name : ".hostname()."\n[".ref($self)."] BatchManager : $BM_CMD\n";
    }
    return $task;
}

=head2 _createLsfTask

 Nom       : _createLsfTask
 Fonction  : Cr�e une tache LSF sp�cifique au domaine sur lequel est �x�cut� le wrapper.
 Exemple   : my $task = $manager -> _createLsfTask($taskName, $cmd, $jobOptsToSubmit);
 Retour    : TASK
 Arguments : STRING STRING STRING. $taskName, $cmd, $jobOptsToSubmit

=cut
sub  _createLsfTask{
    my ($self, $taskName, $cmd, $jobOptsToSubmit) = @_;
    
    my $task;
    
    if(hostdomain() =~ m/.*\.cns\.fr/){
        $task = TaskManager2::TaskLsfCns -> new($taskName, $cmd, $jobOptsToSubmit);
    }elsif(hostdomain() =~ m/.*cng\.fr/){
        $task = TaskManager2::TaskLsfCng -> new($taskName, $cmd, $jobOptsToSubmit);
    }else{
        die "[".ref($self)."] TaskManager n'est pas configur� pour ce domaine avec ce type de BatchManager!\n[".ref($self)."] Host domaine : ".hostdomain()."\n[".ref($self)."] Host name : ".hostname()."\n[".ref($self)."] BatchManager : $BM_CMD\n";
    }
    return $task;
}


1;