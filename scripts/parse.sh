#!/usr/bin/env bash

pidfile=/home/bismark/var/run/parser_pid
#activedir=/home/bismark/var/data/http_uploads/active
activedir="/data/bismark/http_uploads/active /var/local/home/bismark-data-xfer/bismark_data_untarred/active"
datadir=/home/bismark/var/data

# Load local db config file (contains passwords, etc.)
. ~/etc/bdm_db.conf

if [ -e $pidfile ]; then
	echo "parser running"
	exit;
fi
echo $$ > $pidfile
echo "run"
#for i in "$activedir"/OW*; do mv $i/OW* ~/var/data/; done
for i in $activedir; do 
	echo "activedir" $i
	find $i -name "*.xml" -mmin +5 | while read -r f; do
		mv $f $datadir
	done
	#ls $i/OW*/OW* $datadir
done
sleep 5
~/bin/xml_parse_pgsql.py > ~/var/log/last_xml_openwrt_parse.log 2>~/var/log/last_xml_openwrt_parse_error.log
~/bin/fixup_klatch_direction_column.py > ~/var/log/fixup_klatch_direction_column.log 2>&1
rm $pidfile
