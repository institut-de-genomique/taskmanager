################################################################################
#
# * $Id: ProcessManager.pm,v 0.1 2002/08/13 jmaury Exp $
#
# * ProcessManager module for TaskManager2::ProcessManager.pm
# *
# * Copyright Jean-Marc Aury / Institut de Genomique / DSV / CEA
# *                            <jmaury@genoscope.cns.fr>
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

TaskManager2::ProcessManager.pm - Héritage du module CPAN ForkManager, pour lancer des taches en parallele.

=head1 SYNOPSIS

    use TaskManager2::ProcessManager;
    my $pm = new TaskManager2::ProcessManager($max_process);

=head1 DESCRIPTIONs

TaskManager2::ProcessManager est une classe qui fournit une interface pour executer des taches, en utilisant l'appel systéme fork.

=head1 AUTHOR

Jean-Marc AURY, jmaury@genoscope.cns.fr

=cut


package TaskManager2::ProcessManager;

use strict;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

require Exporter;
use lib '/usr/lib/perl5/vendor_perl/5.8.8/';
use Parallel::ForkManager;

@ISA = qw(Exporter Parallel::ForkManager);
# Aucun nom exportï¿½
@EXPORT = ( );

# On exporte aucun nom
@EXPORT_OK = ( );

$VERSION = '0.1';


=head2 new

 Nom       : new
 Fonction  : Crée un objet de la classe ProcessManager.
 Exemple   : $pm = new TaskManager2::ProcessManager($max_process);
 Retour    : une référence sur un objet de la classe TaskManager2::ProcessManager.
 Arguments : un entier (nombre de process maximal pouvant etre crée). 

=cut
sub new {
    my $class = shift;
    my $self = $class->SUPER::new( @_ );
    return $self;
}

=head2 wait_a_child

 Nom       : wait_a_child
 Fonction  : Permet d attendre la fin d un fils.
 Exemple   : $pm->wait_a_child();
 Retour    : le pid du fils terminés.
 Arguments : aucun. 

=cut
sub wait_a_child { 
    my $s = shift;
    $s->on_wait;
    $s->wait_one_child(defined $s->{on_wait_period} ? &WNOHANG : undef);
}
