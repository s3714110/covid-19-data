bash config.sh
hour=$(TZ=Europe/Paris date +%H)

if [ $hour == 7 ] ; then
  cowid check jhu
fi

if [ $hour == 12 ] ; then
  cowid check vax
fi

if [ $hour == 14 ] ; then
  cowid check test
fi
