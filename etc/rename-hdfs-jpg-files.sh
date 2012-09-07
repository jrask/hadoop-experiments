for i in *.jpg-r-00000;
do
   mv "$i" "`basename $i -r-00000`";
done