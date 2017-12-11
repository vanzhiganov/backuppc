#============================================================= -*-perl-*-

# BackupPC::CGI::API package
#
# DESCRIPTION
#
#   This module implements the API action for the CGI interface.
#
# AUTHOR
#   Vyacheslav Anzhiganov  <vanzhiganov@ya.ru>
#
# COPYRIGHT
#   Copyright (C) 2003-2013  Craig Barratt
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#========================================================================
#
# See http://backuppc.sourceforge.net.
#
#========================================================================

package BackupPC::CGI::API;

use strict;
use BackupPC::CGI::Lib qw(:all);
use BackupPC::FileZIO;
use CGI qw/:cgi/;
use List::Util qw(first);
use JSON::PP;

# initialize JSON
my $json = JSON::PP->new->utf8->pretty->allow_nonref;

sub action {
	# TODO: still need to handle authentication

	# methods: GET, POST, DELETE
	my $meth = request_method();
	# sections: hosts, host, backups, backup, archive, jobs
	my $section = $In{section};

	# Get hosts list
	# curl http://localhost/backuppc/index.cgi?action=api&section=hosts
	# Create a new host
	# curl 'http://localhost/backuppc/index.cgi?action=api&section=hosts' -X POST -d 'host=example.hostname.com&dhcp=0&user=root&moreUsers='
	if ( $section == "hosts" ) {
		if ($meth == 'POST') {
                        # create new host
                        my $hostname = $In{host};
                        my $dhcp = 0;
                        my $user = $In{user};
                        my $moreUsers = $In{moreUsers};

			my $host = &get_host_info($In{host});
                        my %output = ( 'host' => $host );

                        encode_response( \%output );
		}

		# get all host names
		my @hosts = &get_hosts("");
		my %output = ( 'hosts' => \@hosts , 'method' => $meth);
		encode_response( \%output );
	}

	# Get host details
	# curl 'http://localhost/backuppc/index.cgi?action=api&section=host&host=example.hostname.com'
	elsif ( $In{api_path} == "host" ) {
		my $paramHost = $In{host};

		if ( $meth !~ /^GET/ ) {
			error_method_not_allowed('GET expected');
		}

		# get host info
		my $host = &get_host_info($patamHost);

		my %output = ( 'host' => $host);

		encode_response( \%output );
	}

	# Get backups list
	# cutl 'http://localhost/backuppc/index.cgi?action=api&section=backups&host=example.hostname.com'
	elsif ( $section == "backups" ) {
		my $paramHost = $In{host};

		if ( $meth !~ /^GET/ ) {
			error_method_not_allowed('GET expected');
		}

		# get host info
		my @hostBackups = get_host_backups($paramHost);
		# 
		my %output = ( 'host_backups' => \@hostBackups );
		# 
		encode_response( \%output );
	}

        # Get latests backup for host
        # cutl 'http://localhost/backuppc/index.cgi?action=api&section=backup&host=example.hostname.com&backup=latest'
	elsif ( $section == "backup" ) {
		my $paramHost = $In{host};

		if ( $meth !~ /^GET/ ) {
			error_method_not_allowed('GET expected');
		}

		# get host info
		my $hostLatestBackup = get_host_latest_backup($paramHost);

		my %output = ( 'host_backup_latest' => $hostLatestBackup );

		encode_response( \%output );
	}

	# archive
	elsif ( $section == "archive" ) {
		# archive: status, latest, start
		my $archive_action = $In{archive};

		# Get archive status
		# curl 'http://localhost/backuppc/index.cgi?action=api&section=archive&archive=status
		if ( $archive_action == "status" ) {
			my $paramArchiveHost = $1;

			if ( $meth !~ /^GET/ ) {
				error_method_not_allowed('GET expected');
			}

			my $archiveStatus = &get_archive_status($paramArchiveHost);
			my %output = ( 'archive_status' => $archiveStatus );
			encode_response( \%output );
		}
		# Get archive latest
		# curl 'http://localhost/backuppc/index.cgi?action=api&section=archive&archive=latest
		elsif ( $archive_action == "latest" ) {

	                my $paramArchiveHost = $1;
        	        my $paramHost        = $2;

	                if ( $meth !~ /^GET/ ) {
        	                error_method_not_allowed('GET expected');
	                }

	                my $lastHostArchive = &get_host_latest_archive( $paramHost, $paramArchiveHost );
        	        my %output = ( 'host_archive_latest' => $lastHostArchive );
	                encode_response( \%output );
		}
		elsif ($archive_action == "start") {
			my $paramArchiveHost = $1;

			if ( $meth !~ /^(?:POST|PUT)/ ) {
				error_method_not_allowed('POST or PUT expected');
			}

			# expected message body:
			# { "hosts": ["host1", "host2", "host3"] }

			my $data = param('POSTDATA')
			  || param('PUTDATA')
			  || error_bad_request('empty request body');
			if ( content_type() !~ /^application\/json(?:|;.*)$/ ) {
				error_unsupported_media_type( 'application/json expected',
					content_type() );
			}
			my $jsonData;
			eval { $jsonData = $json->decode($data) };
			if ($@) {
				error_bad_request('invalid JSON');
			}

			my $reqHosts = $jsonData->{hosts} || error_bad_request('no hosts specified');

			my $reply = start_archive_job( $paramArchiveHost, $reqHosts );
		
			if ($reply !~ /^ok:/) {
				error_internal($reply);
			}

			my %output = ( 'reply' => $reply );

			encode_response( \%output );
		}
	}

	# Get current jobs list
	# curl 'http://localhost/backuppc/index.cgi?action=api&section=jobs'
	elsif ( $section == "jobs" ) {
		if ( $meth !~ /^GET/ ) {
			error_method_not_allowed('GET expected');
		}

		my $jobs = get_jobs();

		encode_response( { 'jobs' => $jobs } );
	}
	else {
		&error_not_found("URI not found action: $In{action} path: $In{api_path}");
	}
}

sub choose_encoding {
	my %myTypes = (
		'application/json' => 0.8,
		'text/plain'       => 1,
	);
	my %acceptedTypes = ();
	foreach my $type ( keys %myTypes ) {

		# filter out types that aren't accepted (Accept() returned undef)
		local $_ = Accept($type);
		$acceptedTypes{$type} = $_ if ($_);
	}

	if ( !%acceptedTypes ) {
		print header( -status => '406 Not Acceptable' );
		exit;
	}

	my $chosenType;

	my $preferredQ     = ( reverse sort values %acceptedTypes )[0];
	my @preferredTypes =
	  grep { $acceptedTypes{$_} == $preferredQ } keys %acceptedTypes;
	if ( @preferredTypes > 1 ) {

	   # client considers more than 1 type equally acceptable, we have to choose
		my %choices =
		  map { $_ => $acceptedTypes{$_} * $myTypes{$_} } @preferredTypes;
		my $chosenQ = ( reverse sort values %choices )[0];
		$chosenType = ( grep { $choices{$_} == $chosenQ } @preferredTypes )[0];
	}
	else {
		$chosenType = shift @preferredTypes;
	}

	if ( !$chosenType ) {
		error_internal('could not decide on content-type');
	}

	return $chosenType;
}

sub encode_response {
	my ( $data, $status ) = @_;
	$status = '200 OK' unless $status;

	my $encoding = choose_encoding();

	if ( $encoding =~ /application\/json/ || $encoding =~ /text\/plain/ ) {
		print header( -type => $encoding, -status => $status );
		print $json->encode($data);
		exit;
	}
	else {
		&error_internal('failed to encode response');
	}
}

sub error_internal {
	my ( $message, $additional_info ) = @_;

	# do not use encode_response() since it could have called this sub
	print header( -status => '500 Internal Error' );
	my %outputData = ( 'error' => { 'message' => $message } );
	if ($additional_info) {
		$outputData{error}->{additional_info} = $additional_info;
	}
	print $json->encode( \%outputData );
	exit;
}

sub error_bad_request {
	my ( $message, $additional_info ) = @_;
	my %outputData = ( 'error' => { 'message' => $message } );
	if ($additional_info) {
		$outputData{error}->{additional_info} = $additional_info;
	}
	encode_response( \%outputData, '400 Bad Request' );
	exit;
}

sub error_method_not_allowed {
	my ( $message, $additional_info ) = @_;
	my %outputData = ( 'error' => { 'message' => $message } );
	if ($additional_info) {
		$outputData{error}->{additional_info} = $additional_info;
	}
	encode_response( \%outputData, '405 Method Not Allowed' );
	exit;
}

sub error_unsupported_media_type {
	my ( $message, $additional_info ) = @_;
	my %outputData = ( 'error' => { 'message' => $message } );
	if ($additional_info) {
		$outputData{error}->{additional_info} = $additional_info;
	}
	encode_response( \%outputData, '415 Unsupported Media Type' );
	exit;
}

sub error_not_found {
	my ( $message, $additional_info ) = @_;
	my %outputData = ( 'error' => { 'message' => $message } );
	if ($additional_info) {
		$outputData{error}->{additional_info} = $additional_info;
	}
	encode_response( \%outputData, '404 Not Found' );
	exit;
}

sub get_hosts {
	my $archiveHost = lc(shift);
	my $hosts       = $bpc->HostInfoRead();
	return grep { lc($_) ne $archiveHost } keys %$hosts;
}

sub get_host_info {
	my $argHost = lc(shift);
	my $host    = $bpc->HostInfoRead($argHost);
	if ( !%$host ) {
		&error_not_found("host \"$argHost\" not found");
	}
	return $host;
}

sub get_host_backups {
	my $argHost = lc(shift);

	return $bpc->BackupInfoRead($argHost);
}

sub get_host_latest_backup {
	my $argHost = lc(shift);

	my @backups      = get_host_backups($argHost);
	my $latestBackup = {};
	if (@backups) {
		$latestBackup =
		  ( reverse sort { $a->{num} <=> $b->{num} } @backups )[0];
	}
	return $latestBackup;
}

sub get_host_latest_archive {
	my ( $argHost, $argArchiveHost ) = map { lc($_) } @_;

	my $host = $bpc->HostInfoRead($argHost);
	if ( !%$host ) {
		&error_not_found("host \"$argHost\" not found");
	}
	my $archiveHost = $bpc->HostInfoRead($argArchiveHost);
	if ( !%$archiveHost ) {
		&error_not_found("archive host \"$argArchiveHost\" not found");
	}

	$host        = $host->{        ( keys %$host )[0] };
	$archiveHost = $archiveHost->{ ( keys %$archiveHost )[0] };
	my $archiveHostName = lc( $archiveHost->{host} );

	my @archiveHostBackups = $bpc->ArchiveInfoRead($archiveHostName);

	my $hostLastBackup = get_host_latest_backup($argHost);

	my $TopDir = $bpc->TopDir();

	# if the host has a backup, let's try and find a matching archive
	my %lastHostArchive = ();
	if ( keys %$hostLastBackup ) {

		# start by finding all the archives that finished successfully
		my @filteredArchives = reverse
		  sort { $a->{num} <=> $b->{num} }
		  grep { $_->{result} eq 'ok' } @archiveHostBackups;

		foreach my $archive (@filteredArchives) {
			%ArchiveReq = ();
			do "$TopDir/pc/$archiveHostName/ArchiveInfo.$archive->{num}"
			  if (
				-f "$TopDir/pc/$archiveHostName/ArchiveInfo.$archive->{num}" );
			my $index = first { lc($_) eq $argHost } @{ $ArchiveReq{HostList} };
			if ($index) {
				my $backupNum = $ArchiveReq{BackupList}[$index];

				$lastHostArchive{archive_info} = $archive;
				$lastHostArchive{archive_req}  = \%ArchiveReq;
				$lastHostArchive{summary}      = {
					'host'            => $argHost,
					'host_backup_num' => $backupNum,
					'archive_num'     => $archive->{num}
				};
				last;
			}
		}
	}

	return \%lastHostArchive;
}

sub get_archive_status {
	my $argArchiveHost = lc( shift @_ );

	GetStatusInfo("jobs");

	my $archiveHost = $bpc->HostInfoRead($argArchiveHost);
	if ( !%$archiveHost ) {
		&error_not_found("archive host \"$argArchiveHost\" not found");
	}

	my %archiveStatus = ( $argArchiveHost => () );

	if ( $Jobs{$argArchiveHost} ) {
		if ( $Jobs{$argArchiveHost}{cmd} =~ /(archiveReq\.[^\s]+)$/ ) {
			my $reqPath = "$TopDir/pc/$argArchiveHost/$1";
			if ( -f $reqPath ) {
				%ArchiveReq = ();
				do $reqPath;

				$archiveStatus{$argArchiveHost}{state}   = 'running';
				$archiveStatus{$argArchiveHost}{reqTime} =
				  $Jobs{$argArchiveHost}{reqTime};
				$archiveStatus{$argArchiveHost}{startTime} =
				  $Jobs{$argArchiveHost}{startTime};
				$archiveStatus{$argArchiveHost}{hostBackups} = {
					map {
						$ArchiveReq{HostList}->[$_] =>
						  $ArchiveReq{BackupList}->[$_]
					  } 0 .. scalar( @{ $ArchiveReq{HostList} } ) - 1
				};
			}
			else {
				error_internal("archiveReq not found: \"$reqPath\"");
			}
		}
		else {
			error_internal(
				"archiveReq not found in cmd: \"$Jobs{$argArchiveHost}{cmd}\"");
		}
	}
	else {
		$archiveStatus{$argArchiveHost}{state} = 'idle';
	}

	return \%archiveStatus;
}

sub start_archive_job {
	my $argArchiveHost = lc( shift @_ );
	my @argHosts       = map { lc($_) } @{ shift @_ || [] };

	my $Hosts    = $bpc->HostInfoRead();
	my $UserName = $bpc->{Conf}{BackupPCUser};
	my $TopDir   = $bpc->{Conf}{TopDir};

	if ( !defined( $Hosts->{$argArchiveHost} ) ) {
		error_bad_request("archive host $argArchiveHost doesn't exist");
	}
	$bpc->ConfigRead($argArchiveHost);

	my ( @HostList, @BackupList );
	foreach my $host (@argHosts) {
		if ( !defined( $Hosts->{$host} ) ) {
			error_bad_request("host $host doesn't exist");
		}

		my @backups = $bpc->BackupInfoRead($host);
		if ( !@backups ) {
			error_bad_request("host $host doesn't have any backups");
		}

		push( @HostList,   $host );
		push( @BackupList, $backups[$#backups]{num} );
	}

	my $ReqFileName;
	for ( my $i = 0 ; ; $i++ ) {
		$ReqFileName = "archiveReq.$$.$i";
		last if ( !-f "$TopDir/pc/$argArchiveHost/$ReqFileName" );
	}
	my %ArchiveReq = (
		archiveloc  => $bpc->{Conf}{ArchiveDest},
		archtype    => 0,
		compression => $bpc->{Conf}{ArchiveComp} eq 'none'
		? $bpc->{Conf}{CatPath}
		: (   $bpc->{Conf}{ArchiveComp} eq 'gzip' ? $bpc->{Conf}{GzipPath}
			: $bpc->{Conf}{Bzip2Path} ),
		compext => $bpc->{Conf}{ArchiveComp} eq 'none' ? ''
		: ( $bpc->{Conf}{ArchiveComp} eq 'gzip' ? '.gz' : '.bz2' ),
		parfile    => $bpc->{Conf}{ArchivePar},
		splitsize  => $bpc->{Conf}{ArchiveSplit} * 1000000,
		host       => $argArchiveHost,
		HostList   => \@HostList,
		BackupList => \@BackupList,
		user       => $UserName,
		reqTime    => time,
	);
	my $archive = Data::Dumper->new( [ \%ArchiveReq ], [qw(*ArchiveReq)] );
	$archive->Indent(1);
	if ( !open( REQ, ">", "$TopDir/pc/$argArchiveHost/$ReqFileName" ) ) {
		error_internal(
"can't open/write request file $TopDir/pc/$argArchiveHost/$ReqFileName"
		);
	}
	binmode(REQ);
	print REQ $archive->Dump;
	close(REQ);
	$bpc->ServerConnect( $bpc->{Conf}{ServerHost}, $bpc->{Conf}{ServerPort} );
	my $reply =
	  $bpc->ServerMesg("archive $UserName $argArchiveHost $ReqFileName");
	$bpc->ServerDisconnect();
	chomp $reply;
	return $reply;
}

sub get_jobs {
	GetStatusInfo("info jobs hosts queueLen");
	return \%Jobs;
}

1;
