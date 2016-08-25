################################################################################
#
# * $Id: ManagerFactory.pm,v 0.1 2014/04/2 akourlai Exp $
#
# * ManagerFactory module for TaskManager2::ManagerFactory.pm
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

TaskManager2::ManagerFactory.pm

=head1 SYNOPSIS

#### LANCER LE WORKFLOW SANS BATCH MANAGER ####

 [user@server] $ workflow.pl

#### LANCER LE WORKFLOW AVEC SLURM ####

 [user@server] $ workflow.pl -BMCMD "msub"
 
#### LANCER LE WORKFLOW AVEC LSF ####
 
 [user@server] $ workflow.pl -BMCMD "bsub"
 
#### SPECIFIER DES OPTIONS AU WORKFLOW ####

 [user@server] $ workflow.pl -BMCMD "msub" -BMOPT "-q large -A fg0001"

 %SLURM_DEFAULT_OPTS  = ( "maxRessource|R=i" => 120, # ressource max pour une tache GLOST
                          "toGlost|G=i" => 120, # nombre de tache sans dependance pour créer une tache GLOST.
                          "nbrTask|n=i" => 1,
                          "nbrCorePerTask|c=i" => 1,
                          "mem_limit|M=i" => undef, # limit per core en Mo
                          "time_limit|T=i" => undef, # temps en seconds.
                          "exclusive|x" => undef,
                          "queue|q=s" => "normal",
                          "projid|A=s" => "fgxxxx",
                          "qos|Q=s" => "normal",
                          "extra_parameters|E=s" => \@tableAddOpts,
                          "starttime|S=s" => undef, # "HH:MM" or "MM/DD HH:MM"
                          "mailopts=s" => undef
                         );

 $LSF_DEFAULT_OPTS = "unset BSUB_QUIET; unset BSUB_QUIET2; BSUB_STDOUT=y; BSUB_STDERR=y;"

 ###########################################################
 #### COMMENT CREER UN WORKFLOW ??? ####
 ###########################################################

#### CREER LE MANAGER ####

 my $runner = TaskManager2::ManagerFactory -> new("NOM_DU_WORKFLOW", "PATH_POUR_LES_LOGS");
 
#### CONFIGURER LE MANAGER ####

 $runner -> setMaxProc(int(MAX_PARALLEL_JOBS)); # max parallels job in workflow (one job -> many tasks -> many cores)

#### CREER UN TACHE ####
    
 $runner -> createJob("NAME", "COMMANDE", "DEPENDANCE", "BATCH_MANAGER_TYPE", "BATCH_MANAGER_OPTIONS");
	
#### FINIR LE WORKFLOW ####
	
 $runner -> run();
	
#### CREER DEUX TACHES ET UNE DEPENDANCE ENTRE CES TACHES ####

 $runner -> createJob("NAME", "sleep 10");  

 $runner -> createJob("NAME2", "sleep 10", "NAME"); # la tache "NAME2" serra execute après la tache "NAME".

#### CREER DEUX TACHES ET UNE DEPENDANCE ENTRE CES TACHES BIS ####

 my $task1 = $runner -> createJob("NAME", "sleep 10");  

 my $task2 = $runner -> createJob("NAME2", "sleep 10");

 $task2 -> addDep($task1); # la tache "NAME2" serra execute après la tache "NAME".
 

#### TRANSMETTRE DES OPTIONS A SLURM POUR UNE TACHE DONNEE: exemple pour MULTI-THREAD ####

 $runner -> createJob("NAME", "sleep 10", undef, "msub", "-n 1 -c 8"); undef car tache sans dependance.

#### TRANSMETTRE DES OPTIONS A LSF POUR UNE TACHE DONNEE: exemple pour preciser la queue ####

 $runner -> createJob("NAME", "sleep 10", undef, "bsub", "-q big");

#### TRANSMETTRE DES OPTIONS A SLURM ou LSF POUR UNE TACHE DONNEE ####

 my @batchs = ("msub", "bsub");
 my %batchsOpts = ("msub" => "-n 1 -c 8", "bsub" => "-q big");
 
 $runner -> createJob("NAME", "sleep 10", undef, \@batchs, \%batchsOpts);
        
 ###########################################################
 #### EXEMPLE DE WORKFLOWS ####
 ###########################################################
 
 #### EXEMPLE 1 : paralleliser un traitement et concatener les resultats ####
 # Faire autant de tris que de fichiers dans le répertoire courant.
 # Chaque tri est multithread sur 8 coeurs.
 # A la fin des tris faire un merge.
 # Au maximun 10 tris serrons effectués en parallel, utilisant au total 80 coeurs.
     
      #!/usr/bin/perl -w
      use strict;
      use TaskManager2::ManagerFactory;
     	
      my $runner = TaskManager2::ManagerFactory -> new("Nom du Workflow", "./");
      $runner -> setMaxProc(10); # max 10 taches exécutées en parallel
      
      my @parallelTasksTab = ();
      my @filesToMerge = ();
      my $cmp = 0;
      
      my @files = glob "*";
      
      # On crée une tache pour chaque des commandes, et on stocke les réferences dans un tableau.
      foreach(@files){
          my $fileIn = $_;
          my $fileOut = $_.".sorted";
          
          my $sortName = "sort".$cmp;
          my $sortCmd = "sort -t 8 -i ".$fileIn." -o ".$fileOut;
          
          push(@parallelTasksTab, $runner -> createJob($sortName, $sortCmd, undef, "msub", "-n 1 -c 8"));
          push(@filesToMerge,$fileOut);
          
          $cmp++;
      }
      
      my $mergeName = "mergeAllSortedFiles";
      my $mergeCmd = "merge ".join(" ", @filesToMerge);
      
      my $mergeTask = $runner -> createJob($mergeName, $mergeCmd);
      $mergeTask -> addDep(\@parallelTasksTab);
     		
      $runner -> run(); # Fin du workflow
       
      #### EXEMPLE 1 : LANCER LE WORKFLOW ####
      # SANS BATCH MANAGER
      [user@server] $ workflow.pl
      
      # AVEC SLURM SUR LA QUEUE LARGE
      [user@SLURM_Server] $ workflow.pl -BMCMD "msub" -BMOPT "-q large"
      
      # AVEC LSF SUR LA QUEUE BIG
      [user@LSF_Server] $ workflow.pl -BMCMD "bsub" -BMOPT "-q big"


=head1 DESCRIPTION

TaskManager2::ManagerFactory.pm est une factory qui permet de choisir le bon manager : TaskManager2::BatchManager.pm ou TaskManager2::ForkManager.pm suivant le serveur a partir du quel le pipeline est lancé ou les options -BMCMD et -BMOPT du pipeline.

=head1 AUTHOR

N'hésitez pas a me contacter pour tout renseignement, ajout de fonctionalité ou signalement de bugs.

Artem Kourlaiev, akourlai@genoscope.cns.fr

=cut

package TaskManager2::ManagerFactory;

use vars qw(@EXPORT @ISA); 
@ISA = qw(Exporter);
@EXPORT = qw(new createTask _toStringWorkflowInfos);

our %SLURM_DEFAULT_OPTS;
our $LSF_DEFAULT_OPTS;

BEGIN {
    use Net::Domain qw(hostdomain hostname); 
    use File::Basename;
    use Getopt::Long;

    sub _toStringWorkflowInfos{
        my $infosToPrint = "";
        $infosToPrint .= "###################################\n";
        $infosToPrint .= "######## WORKFLOW COMMAND #########\n";
        $infosToPrint .= "###################################\n";
        $infosToPrint .= $CMD."\n";
        $infosToPrint .= "###################################\n";
        $infosToPrint .= "######## WORKFLOW OPTIONS #########\n";
        $infosToPrint .= "###################################\n";
        $infosToPrint .= "BMCMD : ".$BM_CMD."\n";
        $infosToPrint .= "BMOPT : ".$BM_OPT."\n";
        $infosToPrint .= "###################################\n";
        $infosToPrint .= "######## WORKFLOW LOCATION ########\n";
        $infosToPrint .= "###################################\n";
        $infosToPrint .= "Host domaine : ".hostdomain()."\n";
        $infosToPrint .= "Host name : ".hostname()."\n";
        return $infosToPrint;
    }
=head2 _get_BMinfos

 Nom       : _get_BMinfos
 Fonction  : Parsing des options -BMCMD et -BMOPT de la ligne de commande, les stocke dans les variables globales $BM_CMD et $BM_OPT.
 Exemple   : _get_BMinfos(\@main::ARGV);
 Retour    : ARRAY, un tableau avec les arguments de la ligne de commande sans les options -BMCMD et -BMOPT.
 Arguments : ARRAY, le tableau des arguments de la ligne de commande.

=cut      
    sub _get_BMinfos{
    	my $a = shift;
    	my @arguments = @{$a};
    	my $i = 0;
    	my @fakeARGV = ();
    	
    	while($i<=$#arguments){
    		if (uc($arguments[$i]) eq "-BMOPT"){
    			$i++;
    			$BM_OPT = $arguments[$i];
    		}elsif (uc($arguments[$i]) eq "-BMCMD"){
    			$i++;
    			if(defined $arguments[$i] and grep( /^$arguments[$i]$/, @bm )){# si le nom du batch manager est correct
    			    $BM_CMD = $arguments[$i];
    			}else{
    			    die "[TaskManager2::ManagerFactory] Le type de Batch Manager specifie par l'option -BMCMD n'est pas gere par TaskManager!\n";
    			}
    		}else{
    			push(@fakeARGV,$arguments[$i]);
    		}
    		$i++
    	}
    	return \@fakeARGV;
    }

=head2 _setSlurmOpts

 Nom       : _setSlurmOpts
 Fonction  : Parsing des options pour SLURM. Les options présents dans la Hash des options pas default sont écrasées par les arguments presents dans $opts.
 Exemple   : _setSlurmOpts($defaultOpts, $opts);
 Retour    : ref HASH. Réference vers une hash avec les options de SLURM pour clé.
 Arguments : ref HASH, STRING. Une référence vers une hash d'options par défault et une chaine de caractère avec les options pour SLURM.

=cut
    sub _setSlurmOpts{
        my ($self, $defaultOpts, $opts) = @_;
        
        my %newOpts = %{$defaultOpts};
        if(defined $opts){
            my @tmpArgs = @main::ARGV;
            
            my @optsTab = ();
            if( $opts =~ /([\w|\W]*)-E '([\w|\W]*)'([\w|\W]*)/ ) { # gestion d'arguements géneriques -E
			    if($1 ne ""){
			    	push (@optsTab, split(" ", $1));
			    }
			    if($2 ne ""){
			    	push (@optsTab, ("-E", $2));
			    }
			    if($3 ne ""){
			    	push (@optsTab, split(" ", $3));
			    }
			    @main::ARGV = @optsTab;
			}else{
				@main::ARGV = split(" ", $opts);
			}
			Getopt::Long::Configure("no_auto_abbrev");
            GetOptions(\%newOpts,
             "maxRessource|R=i",
             "toGlost|G=i",
             "nbrTask|n=i",
             "nbrCorePerTask|c=i",
             "mem_limit|M=i", # per core
             "time_limit|T=i", # secs
             "exclusive|x",
             "queue|q=s",
             "projid|A=s",
             "qos=s", # quality of service 48h ou 72h
             "extra_parameters|E=s",
             "starttime|S=s",
             "mailopts=s") or die  "Argument non compatible avec BRIDGE : ".$opts."\nRegardez dans l'aide de BRIDGE : ccc_msub -h \n";
        
            @main::ARGV = @tmpArgs;
        }
        return \%newOpts;
    }
    
    ####### Comportement par défault #######
    our @bm = ("bsub","-","msub", "glost"); # liste des types de batch manager gérés.
    our ($BM_CMD, $BM_OPT, $CMD) = ("", "", basename($0)." ".join(" ", @main::ARGV)); # options des Batch Manager par défaut.
    our $WORKFLOW_NAME = basename($0);
    
    ####### Extraction de -BMCMD et -BMOPT de la ligne de commande #######
    @main::ARGV = @{_get_BMinfos(\@main::ARGV)};
    
    ####### Les options du wrapper par defaut #######               
    if($BM_CMD eq "msub" || $BM_CMD eq "glost"){  
         #my @tableAddOpts = ();
         my $username = $ENV{LOGNAME} || $ENV{USER} || getpwuid($<);
         %SLURM_DEFAULT_OPTS  = ( "maxRessource" => 120,
                                  "toGlost" => 120,
                                  "nbrTask" => 1,
                                  "nbrCorePerTask" => 1,
                                  "mem_limit" => undef, # limit per core en Mo
                                  "time_limit" => undef, # temps en secs.
                                  "exclusive" => undef,
                                  "queue" => "normal",
                                  "projid" => $username,# options par defaut pour SLURM. Ces options sont utilisées pour générer des taches pour SLURM a partir de createJob(...).
                                  "qos" => undef,
                                  "extra_parameters" => undef,
                                  "starttime" => undef, # "HH:MM" or "MM/DD HH:MM"
                                  "mailopts" => undef
                                 );
                                 
                                 
        if($BM_OPT ne ""){
            %SLURM_DEFAULT_OPTS = %{TaskManager2::ManagerFactory -> _setSlurmOpts(\%SLURM_DEFAULT_OPTS, $BM_OPT)};
        }              
    }else{
        $LSF_DEFAULT_OPTS = $BM_OPT;   
    }
}

use strict;
my $typeBM = "";
my $badBatchManager = "[TaskManager2::ManagerFactory] Option -BMCMD non valide sur ce serveur!\n[TaskManager2::ManagerFactory] -BMCMD : ".$TaskManager2::ManagerFactory::BM_CMD."\n[TaskManager2::ManagerFactory] Host domaine : ".hostdomain()."\n[TaskManager2::ManagerFactory] Host name : ".hostname()."\n";

if(hostdomain() =~ m/.*\.cns\.fr/){
	if($TaskManager2::ManagerFactory::BM_CMD eq "msub" || $TaskManager2::ManagerFactory::BM_CMD eq "glost"){
		#if(hostname() =~ m/etna0/ || hostname() =~ m/etna60/){
		if(1){
			use TaskManager2::BatchManager;
            $typeBM = "batch";
		}else{
			die "[TaskManager2::ManagerFactory] Lancement des pipelines au CNS utilisant TaskManager pour une soumission des taches a SLURM uniquement possible a partir des serveurs d'accueils : etna0\n"."[TaskManager2::ManagerFactory] Type de Batch Manager (-BMCMD) : ".$TaskManager2::ManagerFactory::BM_CMD."\n[TaskManager2::ManagerFactory] Host domaine : ".hostdomain()."\n[TaskManager2::ManagerFactory] Host name : ".hostname()."\n";
		}
	}elsif($TaskManager2::ManagerFactory::BM_CMD eq "bsub"){
		#if(hostname() =~ m/etna1/ || hostname() =~ m/etna2/|| hostname() =~ m/etna48/){
		if(1){
			use TaskManager2::ForkManager;
        	$typeBM = "fork";
		}else{
			die "[TaskManager2::ManagerFactory] Lancement des pipelines au CNS utilisant TaskManager pour une soumission des taches a LSF uniquement possible a partir des serveurs d'accueils : etna1 et etna2\n"."[TaskManager2::ManagerFactory] Type de Batch Manager (-BMCMD) : ".$TaskManager2::ManagerFactory::BM_CMD."\n[TaskManager2::ManagerFactory] Host domaine : ".hostdomain()."\n[TaskManager2::ManagerFactory] Host name : ".hostname()."\n";
		}
	}elsif($TaskManager2::ManagerFactory::BM_CMD eq "-" || $TaskManager2::ManagerFactory::BM_CMD eq ""){
		use TaskManager2::ForkManager;
        $typeBM = "fork";
	}else{
		die $badBatchManager;
	}
}elsif(hostdomain() =~ m/.*\.cea\.fr/){
	if($TaskManager2::ManagerFactory::BM_CMD eq "msub" || $TaskManager2::ManagerFactory::BM_CMD eq "glost"){
		#if(hostname() =~ m/airain70/ || hostname() =~ m/airain71/ || hostname() =~ m/airain72/){
		if(1){
	        use TaskManager2::BatchManager;
	        $typeBM = "batch";
		}else{
			die "[TaskManager2::ManagerFactory] Lancement des pipelines au CCRT en utilisant TaskManager pour une soumission des taches a SLURM uniquement possible a partir des serveurs d'accueils : airain70 - airain71 - airain72 \n"."[TaskManager2::ManagerFactory] Type de Batch Manager (-BMCMD) : ".$TaskManager2::ManagerFactory::BM_CMD."\n[TaskManager2::ManagerFactory] Host domaine : ".hostdomain()."\n[TaskManager2::ManagerFactory] Host name : ".hostname()."\n";
		}
    }elsif($TaskManager2::ManagerFactory::BM_CMD eq "-" || $TaskManager2::ManagerFactory::BM_CMD eq ""){
        use TaskManager2::ForkManager;
        $typeBM = "fork";
    }else{
        die $badBatchManager;
    }
    
}elsif(hostdomain() =~ m/.*cng\.fr/){
	if($TaskManager2::ManagerFactory::BM_CMD eq "msub" || $TaskManager2::ManagerFactory::BM_CMD eq "glost"){
        use TaskManager2::BatchManager;
        $typeBM = "batch";
    }elsif($TaskManager2::ManagerFactory::BM_CMD eq "-" || $TaskManager2::ManagerFactory::BM_CMD eq ""){
        use TaskManager2::ForkManager;
        $typeBM = "fork";
    }else{
        die $badBatchManager;
    }
}else{
	die("Nom de domaine inconnu : ".hostdomain()."\n");
}

=head2 new

 Nom       : new
 Fonction  : Crée un TaskManager suivant les conditions du lancement du wrapper.
 Exemple   : my $runner = TaskManager2::ManagerFactory -> new("Nom du Manager", "./dossierDesLogsDuManager", 10, 1);
 Retour    : OBJECT, une référence vers le Manager crée.
 Arguments : STRING STRING INT INT

=cut
sub new{
    my $self = shift;
    my $manager;
    
    if($typeBM eq "batch"){
        $manager = TaskManager2::BatchManager -> new(@_);
        
        $manager -> setMaxRessource($SLURM_DEFAULT_OPTS{maxRessource});
        $manager -> setLimitToGlost($SLURM_DEFAULT_OPTS{toGlost});
        
    }elsif($typeBM eq "fork"){
        $manager =  TaskManager2::ForkManager -> new(@_);
    }
    # print {$manager -> getOutHandle()} _toStringWorkflowInfos();
    return $manager;
    
}

1;