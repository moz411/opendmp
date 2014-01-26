set -x
mtx -f /dev/sg10 load 1 0

./ndmjob.`./distro.sh` -d6 -c -D localhost/4m,ndmp,ndmp -F /usr/local -T localhost/4m,ndmp,ndmp -f /dev/nst0 -I centos.idx
#./ndmjob.`./distro.sh` -d6 -c -D localhost/4m,ndmp,ndmp -F /boot -T ddve1/4m,ndmp,ndmp -f /dev/dd_st_c0t1l0 -I centos.idx

#./ndmjob.`./distro.sh` -v -c -D opendmp/4m,ndmp,ndmp -F /data01/d01s01 -T localhost/4m,ndmp,ndmp -f /dev/nst0 \
#	-R localhost/4m,ndmp,ndmp -r /dev/sg10 -mE01001@1024 -mE01002@1025 -mE01003@1026 -mE01004@1027 -I moz-freenas.idx
