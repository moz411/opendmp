set -x
NDMJOB="./ndmjob.`./distro.sh`"
mtx -f /dev/sg10 load 1 0
mtx -f /dev/sg10 load 2 1
mtx -f /dev/sg10 load 3 2

#$NDMJOB -d6 -c -D localhost/4m,ndmp,ndmp -F /opt/Python-3.5 -T localhost/4m,ndmp,ndmp -f /dev/nst0 -I centos.idx
$NDMJOB -d6 -c -D localhost/4m,ndmp,ndmp -F /opt/VBoxGuestAdditions-5.0.4 -T localhost/4m,ndmp,ndmp -f /dev/nst0 -I centos0.idx  
#$NDMJOB -d6 -c -D localhost/4m,ndmp,ndmp -F /opt/firefox -T localhost/4m,ndmp,ndmp -f /dev/nst1 -I centos1.idx  
#$NDMJOB -d6 -c -D localhost/4m,ndmp,ndmp -F /none -T localhost/4m,ndmp,ndmp -f /dev/nst2 -I centos2.idx  > centos2.log 2>&1 &

#$NDMJOB -v -c -D opendmp/4m,ndmp,ndmp -F /data01/d01s01 -T localhost/4m,ndmp,ndmp -f /dev/nst0 \
#	-R localhost/4m,ndmp,ndmp -r /dev/sg10 -mE01001@1024 -mE01002@1025 -mE01003@1026 -mE01004@1027 -I moz-freenas.idx
