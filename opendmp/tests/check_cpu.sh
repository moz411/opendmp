while /bin/true
do 
top -H -b -n 1 -p `pidof python3.4 |tr ' ' ','` 
top -b -n 1 -p `pidof star |tr ' ' ','`
echo
sleep 1
 done
