#!/usr/bin/perl -w

package main;

use strict;
use Getopt::Long;

use TaskManager2::ManagerFactory;

our $PRG_NAME = $0;

my ($FILE, $MAX_PROCESS, $LOG, $EXAMPLE, $VERBOSE, $HELP) = ("", 10, "./", 0, 0, 0); 
my $result = &GetOptions(
			 "f=s"      => \$FILE,
			 "n=i"      => \$MAX_PROCESS,
			 "log=s"    => \$LOG,
			 "example"  => \$EXAMPLE,
			 "v"        => \$VERBOSE,
			 "h"        => \$HELP
			 );

if (!$result) { error(); }
if($EXAMPLE) { example(); }
if ($HELP || $FILE eq "") { usage(); }

warn "Creating the TaskManager ...\n" if $VERBOSE;
my $runner = TaskManager2::ManagerFactory->new("tm_logs", $LOG);

$runner -> setMaxProc($MAX_PROCESS);
warn "TaskManager created.\n" if $VERBOSE;

warn "Loading the configuration file ...\n" if $VERBOSE;
$runner->load($FILE);
warn "Configuration file loaded.\n" if $VERBOSE;

warn "Launching the TaskManager ...\n" if $VERBOSE;
$runner->run();

sub error {
    my $msg = shift;
    if(defined $msg) { warn "$msg\n"; }
    warn "See $PRG_NAME -h for more details.\n";
    exit 1;
}

sub usage {
    my $msg = shift;
    if (defined $msg && $msg ne "") {
	warn $msg,"\n";
	}
    
my $usage = "
+--------------------------------------------------------------------------------+
|$PRG_NAME  -f <file> -n <int>                                                
|            -f       : Fichier de configuration                                 |
|            -n       : Nombre de process en parallel, defaut=10                 |
|            -log     : Repertoire de log, defaut=./                             |
|            -example : Imprime un exemple de fichier de configuration           |
|            -v       : verbose                                                  |
|            -h       : Aide                                                     |
+--------------------------------------------------------------------------------+\n";
    die $usage;
}

sub example {
    print "
# task JOB_NAME : DEPENDANCE
# COMMANDE
#
# Le job job2 demarre lorsque le job job1 est termine
#
task job1 :
echo toto
task job2 : job1
ls ~
";
    exit;
}