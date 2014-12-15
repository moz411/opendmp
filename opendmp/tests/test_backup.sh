set -x
NDMJOB="./ndmjob.`./distro.sh`"
ssh mhvtl mtx -f /dev/sg6 load 1 0

$NDMJOB -d6 -c -D localhost/4m,ndmp,ndmp -F /usr -T mhvtl/4m,ndmp,ndmp -f /dev/nst0 -I centos.idx
#$NDMJOB -d6 -c -D localhost/4m,ndmp,ndmp -F /boot -T ddve1/4m,ndmp,ndmp -f /dev/dd_st_c0t1l0 -I centos.idx

#$NDMJOB -v -c -D opendmp/4m,ndmp,ndmp -F /data01/d01s01 -T localhost/4m,ndmp,ndmp -f /dev/nst0 \
#	-R localhost/4m,ndmp,ndmp -r /dev/sg10 -mE01001@1024 -mE01002@1025 -mE01003@1026 -mE01004@1027 -I moz-freenas.idx
