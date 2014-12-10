NDMJOB="./ndmjob.`./distro.sh`"
$NDMJOB -v -q -D localhost/4m,ndmp,ndmp
$NDMJOB -q -T mhvtl/4m,ndmp,ndmp -f /dev/nst0
$NDMJOB -q -R mhvtl/4m,ndmp,ndmp -r /dev/sg6
