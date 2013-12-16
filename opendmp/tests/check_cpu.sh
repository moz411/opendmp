while /bin/true
do 
top -H -b -n 1 -p `pidof python3.3 |tr ' ' ','`  | grep `id -un`
top -b -n 1 -p `pidof star |tr ' ' ','` | grep `id -un`
echo
sleep 1
 done
