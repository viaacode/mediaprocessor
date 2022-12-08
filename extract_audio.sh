#!/usr/bin/env bash
# This was first attempted to implement in python, but this proved not possible
# because the request library does not play well with the swarm object store

# We need a dedicated file descripter for the input file
# Using stdin interferes with ffmpeg-curl pipe
exec 3<browses.txt

# We use file descripter 4 for errot/warning messages. stderr is 
# used for debug info.
exec 4>error.out

# Wait for subshells to complete and serialise the logs
function wait_and_log {
	wait
	cat out.* 2>/dev/null
	cat dbg.* >&2 2>/dev/null
	cat err.* >&4 2>/dev/null
	clean_logs
}

function clean_logs {
	rm err.* out.* dbg.* 2>/dev/null
}

SourceUrl=
TargetBaseUrl=
TargetDomain=
TargetBucket=

clean_logs
i=0
while read -r -u3 Name; do
	if ! curl -s -I --fail "$SourceUrl/$Name" -o /dev/null ; then
		echo "Not Found: $Name"
		echo "Not Found: $Name" >&4
		continue
	fi
	if ! (mediainfo "$SourceUrl/$Name" | grep -q Audio); then
		echo "No Audio: $Name" 
		echo "No Audio: $Name" >&4
		continue
	fi
	if [ $i -ge 16 ]; then
		echo waiting
		wait_and_log
		i=0
	fi
	i=$((i+1))
	echo "Extracting Audio: $Name"
	# Start a subshell for transcoding and upload
	# Every subshell logs in its own file in order to have a clean ordered log
	# The main script waits for the subshells to complete and serialises the logs
	( TargetAudName="${Name%.mp4}.aac"
	tid=$(printf '%03d' $i)
	echo "$Name" >>dbg.$tid
	/usr/bin/ffmpeg -loglevel error -i $SourceUrl/$Name  -vn -acodec copy -f adts pipe:1 2>>dbg.$tid |\
		curl -s --fail -X POST --post301 --location-trusted  \
		-H 'transfer-encoding: chunked'  -H 'expect: 100-continue' --data-binary @- \
		-H 'Content-Type: audio/aac' \
		"$TargetBaseUrl/$TargetBucket/$TargetAudName?domain=$TargetDomain" >>dbg.$tid 2>&1
		if [ $? -eq 0 ] ; then
		       	echo "Posted $TargetAudName" >>out.$tid
		else
			echo "Error Posting $TargetAudName" >>err.$tid
			echo "Error Posting $TargetAudName" >>out.$tid
		fi ) &

done
# Wait for remaing threads to complete and clean up
wait_and_log
