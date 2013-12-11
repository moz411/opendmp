while /bin/true
do 
top -H -b -n 1 -p `pidof python3.3 |tr ' ' ','`  | grep moz
top -b -n 1 -p `pidof star |tr ' ' ','` | grep moz
echo
sleep 1
 done
