while /bin/true
do
netstat -an |grep tcp |grep 1000
echo
sleep 1
done

